# Git-Xet: Scale Git to Terabyte-sized Repos

Git Xet is a plugin for git that allows git repositories to scale to terabytes of data.  

## Design Goals

### Binary Content Deduplication

Binary content is deduplicated across commits, files, and blocks of identical content in otherwise different files.   This allows git to efficiently work with repositories containing large datasets that evolve over time.  See our paper at CIDR 2023, [Git is for Data](https://www.cidrdb.org/cidr2023/papers/p43-low.pdf), for more information on the technical details of this process.  

### Ease of Use

Once a repo is set up to use git-xet, nothing more is required. No per-file configuration or tracking is necessary.

### Scalability

Using the `git xet mount` feature, any branch or commit can be mounted locally as a read-only folder.  The data in this folder is seamlessly materialized as needed, allowing large repositories to be viewed quickly.  This is built using our parallel project, [nfsserve](https://github.com/xetdata/nfsserve).

### Flexibility 

Git-xet naturally integrates seamlessly with [xethub](https://about.xethub.com/?), which provides a number of nice perks, such as data analytics and integration with tools like [xetcache](https://github.com/xetdata/xetcache) and [pyxet](https://pyxet.readthedocs.io/en/latest/) for easy access to your data from any environment.  It also integrates with GitHub or can be used completely localling against a local data store.  

### Open Source

Finally, git xet is an open source tool that is free to use and build upon.  By default, the binary data content is stored on xethub and backed by S3.  It can also be configured against a local data store for a fully free and open source route to managing large repositories. 

## Documentation 

Documentation for git-xet, pyxet, and the xet cli is available on xethub [here](https://xethub.com/assets/docs/).

## Installation and Development

The easiest way to install git-xet is to install one of the prebuilt [binaries](https://github.com/xetdata/xet-tools/releases).  Once git-xet is in your path, 

To build git-xet from source, [install the rust toolchain](https://doc.rust-lang.org/cargo/getting-started/installation.html), then run `cargo build --release` in the the `rust/` subfolder.  The `git-xet` binary is then in `rust/targets/release/`.  After this, run `git-xet install` to set the proper git config settings.

### Get involved

We will develop this package in public under the BSD license and welcome contributions.

Join our [Discord](https://discord.gg/KCzmjDaDdC) to get involved. To stay informed about updates, star this repo and sign up at [XetHub](https://xethub.com/user/sign_up) to get the newsletter.


## Optional: XetHub Account Setup

The use of the fully managed [XetHub](about.xethub.com) service provides many perks, including reliable data storage and seamless minimal-configuration integration with git.

### Authentication

Signup on [XetHub](https://xethub.com/user/sign_up) and obtain a username and personal access token. You should save this token for later use, or go to https://xethub.com/user/settings/pat to create a new token. 

There are two ways to authenticate with XetHub:

#### Command Line

Run the command given when you create your personal access token:

```bash
git xet login -e <email> -u <username> -p <personal_access_token>
```
git xet login will write authentication information to `~/.xetconfig`

#### Environment Variables

Environment variables may be sometimes more convenient:

```bash
export XET_USER_EMAIL = <email>
export XET_USER_NAME = <username>
export XET_USER_TOKEN = <personal_access_token>
```
