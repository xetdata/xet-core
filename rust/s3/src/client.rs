use more_asserts::debug_assert_lt;

use std::{
    self,
    fs::{create_dir_all, OpenOptions},
    io::{Seek, SeekFrom, Write},
    ops::{DerefMut, Range},
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use parutils::tokio_par_for_each;
use pbr::ProgressBar;
use rusoto_s3::{
    GetBucketLocationRequest, GetObjectError, GetObjectRequest, ListObjectsV2Request, Object, S3,
};
use tokio::{io::AsyncReadExt, sync::Mutex};
use tokio_retry::strategy::{jitter, FibonacciBackoff};
use tokio_retry::RetryIf;
use tracing::{error, info, warn};

use rusoto_core::{Region, RusotoError};

use crate::errors::{convert_parallel_error, Result, XetS3Error};

use crate::utils::{parse_s3_url, to_range_header};

/// Default from backoff crate is 15 minutes, which is way too long. Capping to 2 minutes instead
const MAX_RETRY_BACKOFF_TIME_MILLIS: u64 = 1_000;
const MAX_RETRY_BACKOFF_NUM_TRIES: u64 = 5;
const DOWNLOAD_QUEUE_NUM_WORKERS: usize = 16;
const DOWNLOAD_WORK_CHUNK: usize = 16 * 1024 * 1024;

/// An interface to downloading S3 Client stuff
pub struct S3Client {
    s3_client: rusoto_s3::S3Client,
}

impl S3Client {
    /// Open an s3 client for a specific bucket, determining the region
    /// for that connection.
    pub async fn open_for_bucket(bucket: &str) -> Result<S3Client> {
        let current_region = Region::default();
        let mut s3_client = rusoto_s3::S3Client::new(current_region.clone());

        // Get the region
        let region_info = s3_client
            .get_bucket_location(GetBucketLocationRequest {
                bucket: bucket.to_owned(),
                expected_bucket_owner: None,
            })
            .await
            .map_err(|e| {
                let err_str = format!(
                    "Error retrieving info for S3 bucket {:?} : {:?}",
                    &bucket, e
                );
                error!("Error: {}", &err_str);
                XetS3Error::Error(err_str)
            })?;

        if let Some(r) = region_info.location_constraint {
            if r != current_region.name() {
                match Region::from_str(&r) {
                    Ok(r) => {
                        info!("Using region {:?} for bucket {:?}.", r, &bucket);
                        s3_client = rusoto_s3::S3Client::new(r);
                    }
                    Err(e) => {
                        warn!(
                            "Unknown error setting region to {:?}, ignoring: {:?}.",
                            r, e
                        );
                    }
                }
            }
        }

        Ok(S3Client { s3_client })
    }

    /// Retrieve an object
    async fn get_object(
        &self,
        bucket: &str,
        object_key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Vec<u8>> {
        let range = range.map(to_range_header);

        let range_ref = &range;
        let retry_strategy = FibonacciBackoff::from_millis(MAX_RETRY_BACKOFF_TIME_MILLIS)
            .map(jitter) // add jitter to delays
            .take(MAX_RETRY_BACKOFF_NUM_TRIES as usize); // limit to 3 retries

        let obj = RetryIf::spawn(
            retry_strategy,
            || async move {
                self.s3_client
                    .get_object(GetObjectRequest {
                        bucket: bucket.to_owned(),
                        key: object_key.to_owned(),
                        range: range_ref.clone(),
                        ..Default::default()
                    })
                    .await
            },
            |e: &RusotoError<_>| !matches!(e, RusotoError::Service(GetObjectError::NoSuchKey(_))),
        )
        .await
        .map_err(|e| XetS3Error::Error(format!("Error retrieving object: {e:?}:")))?;

        let mut buffer = Vec::new();
        let _size = obj
            .body
            .ok_or_else(|| XetS3Error::Error("S3 returned no body".to_string()))?
            .into_async_read()
            .read_to_end(&mut buffer)
            .await;
        Ok(buffer)
    }

    /// List out the contents of an s3 bucket, returning them as a list of objects.
    pub async fn list_objects(&self, bucket: &str, prefix: Option<&str>) -> Result<Vec<Object>> {
        let mut ret: Vec<Object> = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let objects = self
                .s3_client
                .list_objects_v2(ListObjectsV2Request {
                    bucket: bucket.to_owned(),
                    continuation_token,
                    delimiter: None, // This means that we simply get a list of the objects.
                    encoding_type: None,
                    expected_bucket_owner: None, // Todo -- we probably need to plumb this through.
                    fetch_owner: None,
                    max_keys: None,
                    prefix: prefix.map(|p| p.to_owned()),
                    request_payer: None,
                    start_after: None,
                })
                .await
                .map_err(|e| {
                    XetS3Error::Error(format!("Error syncing objects from S3 Bucket: {e:?}"))
                })?;

            if let Some(contents) = objects.contents {
                ret.extend_from_slice(&contents);
            }

            if objects.is_truncated.unwrap_or(false) {
                continuation_token = objects.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(ret)
    }

    /// Synchronize an s3 url to a destination folder.
    ///
    /// The s3 url must be of the form s3://<bucket>/<key>.
    pub async fn sync_to_local(&self, bucket: &str, prefix: &str, dest: &PathBuf) -> Result<()> {
        // Declare some local structs for the download work queue
        #[derive(Clone, Default)]
        enum DownloadAction {
            #[default]
            FullFile,
            PartialFile {
                current_part: Arc<Mutex<usize>>,
                num_parts: usize,
                download_completion_counter: Arc<AtomicUsize>,
            },
        }

        #[derive(Default, Clone)]
        struct DownloadUnit {
            key: String,
            dest_file: PathBuf,
            dest_temp_file: PathBuf,
            size: usize,
            action: DownloadAction,
        }

        let mut work_queue: Vec<DownloadUnit> = Vec::new();

        let mut total_size: usize = 0;
        let mut total_num_objects: usize = 0;

        let objects = self.list_objects(bucket, Some(prefix)).await?;

        for obj in objects {
            let dest_dir: Option<PathBuf>;
            let dest_file;
            let dest_temp_file;
            let key: String;

            if let Some(k) = obj.key {
                key = k;
                dest_file = dest.join(&key);
                dest_dir = dest_file.parent().map(|d| d.to_path_buf());

                // Store the dest temp file in .<filename>.part, rename at end.
                dest_temp_file = dest_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from_str("./").unwrap())
                    .join(
                        ".".to_owned()
                            + dest_file.file_name().unwrap().to_str().unwrap()
                            + ".download",
                    );
            } else {
                info!("Skipping object {:?} as key None.", obj);
                continue;
            }

            if let Some(dir) = &dest_dir {
                create_dir_all(dir)?;
            }

            let size = obj.size.unwrap_or(0) as usize;

            // TODO: Skip files when present, mod time matches , and size matches.
            total_size += size;
            total_num_objects += 1;

            // Create the object file, if it's not present.
            if size >= DOWNLOAD_WORK_CHUNK {
                // This will be downloaded in multiple parts.
                let part_counter = Arc::new(Mutex::new(0usize));
                let num_parts = (size + (DOWNLOAD_WORK_CHUNK - 1)) / DOWNLOAD_WORK_CHUNK; // Equivalent of ceiling
                let completion_counter = Arc::new(AtomicUsize::new(0));

                let download_unit = DownloadUnit {
                    key: key.clone(),
                    dest_file: dest_file.clone(),
                    dest_temp_file: dest_temp_file.clone(),
                    size,
                    action: DownloadAction::PartialFile {
                        current_part: part_counter.clone(),
                        num_parts,
                        download_completion_counter: completion_counter.clone(),
                    },
                };

                // Append num_parts to the end of the work queue, using clones of the download unit.
                work_queue.resize_with(work_queue.len() + num_parts, || download_unit.clone());
            } else {
                work_queue.push(DownloadUnit {
                    key: key.clone(),
                    dest_file: dest_file.clone(),
                    dest_temp_file: dest_temp_file.clone(),
                    size,
                    action: DownloadAction::FullFile,
                });
            }
        }

        println!("Syncing {total_size} bytes across {total_num_objects} objects.");

        let prb = {
            let mut prb = ProgressBar::on(std::io::stderr(), total_size as u64);
            prb.set_units(pbr::Units::Bytes);
            Arc::new(Mutex::new(prb))
        };
        let prb_ref = &prb;
        let num_units = work_queue.len();

        let bucket_ref = &bucket;
        tokio_par_for_each(
        work_queue,
        DOWNLOAD_QUEUE_NUM_WORKERS,
        |obj, i| async move {
            match &obj.action {
                DownloadAction::FullFile => {
                    info!(
                        " Download Unit {:?} / {:?}: Downloading file {:?}: s3://{:?}/{:?}, size={:?}",
                        i, &num_units, &obj.dest_file, &bucket_ref, &obj.key, &obj.size
                    );

                    let data = self.get_object(bucket_ref, &obj.key, None).await?;

                    let mut writer = OpenOptions::new().create(true).write(true).open(&obj.dest_file)?;

                    writer.write_all(&data)?;
                    prb_ref.lock().await.add(obj.size as u64);
                }
                DownloadAction::PartialFile {
                    current_part,
                    num_parts,
                    download_completion_counter,
                } => {
                    let part: usize;

                    // Grab the part count mutex, registering the proper chunk.
                    {
                        let mut part_lock = current_part.lock().await;
                        let part_ref: &mut usize = part_lock.deref_mut();
                        part = *part_ref;

                        debug_assert_ne!(part, *num_parts);

                        if part == 0 {
                            // We're the first thread to access the file, so create parts in a temporary file,
                            // then move it over when it's done.
                            info!(
                                " Download Unit {:?} / {:?}: Allocating empty file {:?} for s3://{:?}/{:?}, size={:?}",
                                i, &num_units, &obj.dest_temp_file, &bucket_ref, &obj.key, &obj.size
                            );

                            // Set the length of the file to be the correct size so
                            // that all the threads can download it in parallel.
                            OpenOptions::new()
                                .write(true)
                                .create(true)
                                .open(&obj.dest_temp_file)?
                                .set_len(obj.size as u64)?;
                        }

                        // Increase the counter
                        *part_ref += 1;
                    } // Release the lock

                    // Now, retrieve the data for that particual part.
                    let obj_start = part * DOWNLOAD_WORK_CHUNK;
                    let obj_end = usize::min((part + 1) * DOWNLOAD_WORK_CHUNK, obj.size);

                    if part == (*num_parts) - 1 {
                        assert_eq!(obj_end, obj.size);
                    } else {
                        debug_assert_lt!(obj_start, obj_end);
                        debug_assert_lt!(obj_end, obj.size);
                    }

                    info!(
                        " Download Unit {:?} / {:?}: Downloading part {:?} / {:?} of file {:?}: s3://{:?}/{:?}, range={:?}-{:?}",
                        i, &num_units, &part, num_parts, &obj.dest_file, &bucket_ref, &obj.key, &obj_start, &obj_end
                    );

                    let data = self.get_object(
                        bucket_ref,
                        &obj.key,
                        Some(Range {
                            start: obj_start as u64,
                            end: obj_end as u64,
                        }),
                    )
                    .await?;

                    assert_eq!(data.len(), obj_end - obj_start);

                    let mut writer = OpenOptions::new().create(true).write(true).open(&obj.dest_file)?;

                    writer.seek(SeekFrom::Start(obj_start as u64))?;

                    writer.write_all(&data)?;

                    prb_ref.lock().await.add((obj_end - obj_start) as u64);

                    {
                        // Now, are we the last one to finish up?
                        let current_completed_part =
                            download_completion_counter.fetch_add(1, Ordering::Acquire);

                        debug_assert_lt!(current_completed_part, *num_parts);

                        if current_completed_part == (*num_parts - 1) {
                            // In this case, all the other workers have written out their
                            // data already, so the .download file is complete.

                            info!(
                                " Download Unit {:?} / {:?}: s3://{:?}/{:?} file complete, renaming {:?} to {:?}.",  
                                i, &num_units,  &bucket_ref, &obj.key, &obj.dest_temp_file, &obj.dest_file
                            );

                            std::fs::rename(obj.dest_temp_file, obj.dest_file)?;
                        }
                    }
                }
            }

            Result::<_>::Ok(())
        },
    )
    .await
    .map_err(convert_parallel_error)
    .map_err(|e| {
        let err_str = format!(
            "Error downloading S3 bucket {:?} to {:?}: {:?}",
            &bucket, &dest, e
        );
        error!("Error: {}", &err_str);
        XetS3Error::Error(err_str)
    })?;

        prb_ref.lock().await.finish();

        Ok(())
    }
}

pub async fn sync_s3_url_to_local(url: &str, dest: &PathBuf) -> Result<()> {
    let (bucket, prefix) = parse_s3_url(url)?;

    let s3_client = S3Client::open_for_bucket(&bucket).await?;

    s3_client.sync_to_local(&bucket, &prefix, dest).await?;

    Ok(())
}
