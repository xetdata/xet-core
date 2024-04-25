# Mount

PyXet allows you to perform a filesystem mount of any version of any Xet repository
allowing you to immediately access TBs of data anywhere without having to download 
everything.

This lets you use any tool that works with local files, but with data stored in
XetHub.  This is perfect for exploring larger images and text files, as well as 
SQLite and Parquet databases.

To mount:
```bash
xet mount xet://<username>/<repo>/<branch> <local_path>
```

On windows, the `local_path` must be a drive letter. For instance `X:`

For instance, you can mount the Flickr30k dataset with:

```bash
xet mount xet://XetHub/Flickr30k/main Flickr30k
```
And you will be able to browse to it and explore its contents.

## Prefetch

As a slightly larger example, you can mount the Laion400M metadata (54GB) with 
```bash
xet mount --prefetch 0 xet://XetHub/LAION-400M/main LAION400M 
cd LAION400M
```
which provides a collection of Parquet files which you can query
easily with duckdb. For instance:

```python
import duckdb
# count the number of rows
duckdb.query("select COUNT(*) from 'data/*.parquet'")
# See the distribution of licenses
duckdb.query("select LICENSE, count() as COUNT from 'data/*.parquet' 
        group by LICENSE order by COUNT desc").df()
```

The *prefetch* argument allows you to tune between random access and continuous
streaming of files (for instance if you are doing ML training, or need to
quickly download large files). `prefetch=0` is good for random access
such as for duckdb queries, or for SQLite queries. The default prefetch
value of 32 is good for bulk file access.


## Many More

Go to [XetHub's explore page](https://xethub.com/explore/) to find more datasets
and models you can mount and explore right away!
