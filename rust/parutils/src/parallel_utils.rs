use std::mem::take;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub enum ParallelError<E> {
    JoinError,
    TaskError(E),
}

///  Call an async closure in parallel within the tokio runtime, with one call for each index in 0..n_tasks.  
///
///  Usage:
///   
///  ```ignore
///  let v = vec![...];
///  let v_ref = &v_ref; // Move is used, so capture non-move context with references.
///
///  run_tokio_parallel(v.len(), |idx| async move {
///     // Don't actually do this
///     let x = &v_ref[i];
///     // do something
///  })?;
///  ```
///
///
///   Note:  Use Arc<Mutex<...>> around writable things.
///
pub async fn run_tokio_parallel<F, R, E>(
    n_tasks: usize,
    max_concurrent: usize,
    f: F,
) -> Result<(), ParallelError<E>>
where
    F: Send + Sync + Fn(usize) -> R,
    R: futures::Future<Output = Result<(), E>> + Send,
    E: Send + Sync + 'static,
{
    let proc_queue = Arc::new(AtomicUsize::new(0usize));

    let (_, outputs) = async_scoped::TokioScope::scope_and_block(|scope| {
        for _ in 0..max_concurrent {
            let proc_queue = proc_queue.clone();
            let n_tasks = n_tasks;
            let f = &f;

            scope.spawn(async move {
                loop {
                    let idx = proc_queue.fetch_add(1, Ordering::Relaxed);

                    if idx >= n_tasks {
                        return Result::<(), E>::Ok(());
                    }

                    f(idx).await?;
                }
            });
        }
    });

    for o in outputs {
        match o {
            // this is a tokio join error
            Err(_) => Err(ParallelError::<E>::JoinError)?,
            Ok(Err(e)) => Err(ParallelError::<E>::TaskError(e))?,
            _ => (),
        }
    }

    Ok(())
}

///  Call an async closure in parallel within the tokio runtime, with one call for each index in 0..n_tasks.  
///
///  Usage:
///  ```ignore
///  let v_in : Vec<InputType> = vec![...];
///
///  let v_out : Vec<OutputType> =
///      run_tokio_par_for_each(v_in, |(item : InputType, idx : usize)| async move {
///     
///     // do something to item
///     let out : OutputType = ...
///     return Ok(out)
///  })?;
///  ```
///
pub async fn tokio_par_for_each<F, I, R, Q, E>(
    input: Vec<I>,
    max_concurrent: usize,
    f: F,
) -> Result<Vec<Q>, ParallelError<E>>
where
    F: Send + Sync + Fn(I, usize) -> R,
    I: Send + Default,
    R: futures::Future<Output = Result<Q, E>> + Send,
    Q: Default + Send,
    E: Send + Sync + 'static,
{
    let mut _output: Vec<Q> = Vec::with_capacity(input.len());
    for _ in 0..input.len() {
        _output.push(Q::default());
    }
    let n_tasks = input.len();
    let proc_queue = Arc::new(Mutex::<(usize, Vec<I>)>::new((0usize, input)));
    let proc_result = Arc::new(Mutex::new(_output));

    let (_, outputs) = async_scoped::TokioScope::scope_and_block(|scope| {
        for _ in 0..max_concurrent {
            let proc_queue = proc_queue.clone();
            let proc_result = proc_result.clone();
            let n_tasks = n_tasks;
            let f = &f;

            scope.spawn(async move {
                loop {
                    let idx;
                    let task: I;

                    {
                        let mut obj = proc_queue.lock().await;
                        let stats = &mut obj;
                        idx = stats.0;
                        if idx >= n_tasks {
                            return Result::<(), E>::Ok(());
                        } else {
                            task = take(&mut stats.1[idx]);
                            stats.0 += 1;
                        }
                    }

                    let result = f(task, idx).await?;

                    {
                        let mut obj = proc_result.lock().await;
                        obj[idx] = result;
                    }
                }
            });
        }
    });

    // TODO: duplicate with line 62-69
    for o in outputs {
        match o {
            // this is a tokio join error
            Err(_) => Err(ParallelError::<E>::JoinError)?,
            Ok(Err(e)) => Err(ParallelError::<E>::TaskError(e))?,
            _ => (),
        }
    }

    let mut obj = proc_result.lock().await;
    Ok(take(&mut obj))
}

#[cfg(test)]
mod parallel_tests {

    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_parallel() {
        let atomic_ctr = AtomicUsize::new(0);
        run_tokio_parallel(400, 4, |_| async {
            atomic_ctr.fetch_add(1, Ordering::Relaxed);
            Result::<_, anyhow::Error>::Ok(())
        })
        .await
        .unwrap();

        assert_eq!(atomic_ctr.load(Ordering::SeqCst), 400);
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn test_simple_parallel() -> Result<(), Box<dyn std::error::Error>> {
        let data: Vec<String> = (0..400)
            .map(|i| format!("Number = {}", &i))
            .collect();

        let data_ref: Vec<String> = data
            .iter()
            .enumerate()
            .map(|(i, s)| format!("{}{}{}", &s, ":", &i))
            .collect();

        // let data_test: Vec<String> =
        let r = tokio_par_for_each(data, 4, |s, i| async move {
            Result::<_, anyhow::Error>::Ok(format!("{}{}{}", &s, ":", &i))
        })
        .await
        .unwrap();

        assert_eq!(data_ref.len(), r.len());
        for i in 0..data_ref.len() {
            assert_eq!(data_ref[i], r[i]);
        }

        Ok(())
    }
}
