[![No Maintenance Intended](http://unmaintained.tech/badge.svg)](http://unmaintained.tech/)

# [DEPRECATED] Git-Xet: Scale Git to Terabyte-sized Repos

**_XetHub has joined [Hugging Face 🤗](https://huggingface.co/blog/xethub-joins-hf). Follow our work to improve large scale collaboration on [Hugging Face Hub](https://huggingface.co/xet-team)._**

----

Git-Xet is a plugin for Git that allows Git repositories to scale to terabytes of data.  

## Design Goals

### Content Deduplication

Content -- both binary and larger text-based files -- are deduplicated across commits, files, and blocks of identical content in otherwise different files.   This allows git to efficiently work with repositories containing large datasets that evolve over time.  See our blog post [here](https://about.xethub.com/blog/git-is-for-data-published-in-cidr-2023) for more information on the technical details of this process, including a link to our CIDR 2023 paper.

### Ease of Use

Once a repo is set up to use Git-Xet, nothing more is required. No per-file configuration or tracking is necessary.

### Scalability

Using the `git xet mount` feature, any branch or commit can be mounted locally as a read-only folder.  The data in this folder is seamlessly materialized as needed, allowing large repositories to be viewed quickly.  This is built using our parallel project, [nfsserve](https://github.com/xetdata/nfsserve).

### Flexibility 

Git-Xet naturally integrates seamlessly with [XetHub](https://about.xethub.com/?), which provides a number of nice perks, such as data analytics and integration with tools like [xetcache](https://github.com/xetdata/xetcache) for easy access to your data from any environment. Or just use Git-Xet locally against a local, private data store.  

### Open Source

Finally, Git-Xet is an open source tool that is free to use and build upon.  By default, the binary data content is stored on XetHub and backed by S3. It can also be configured against a local data store for a fully free and open source route to managing large repositories.

## Installation and Development

The easiest way to install Git-Xet is to install one of the prebuilt [binaries](https://github.com/xetdata/xet-tools/releases). Once the `git-xet` executable is in your path, run `git-xet install` from the command line to configure the git config settings, then any enabled repository will work. 

To build Git-Xet from source, [install the rust toolchain](https://doc.rust-lang.org/cargo/getting-started/installation.html), then run `cargo build --release` in the `rust/` subfolder.  The `git-xet` binary is then in `rust/target/release/`.  After this, run `git-xet install` to set the proper git config settings.

### Get involved

We will develop this package in public under the BSD license and welcome contributions.
