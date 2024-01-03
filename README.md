# Git-Xet: Scale Git to Terabyte-sized Repos

Git-Xet is a plugin for git that allows git repositories to scale to terabytes of data.  

## Design Goals

### Content Deduplication

Content -- both binary and larger text-based files -- are deduplicated across commits, files, and blocks of identical content in otherwise different files.   This allows git to efficiently work with repositories containing large datasets that evolve over time.  See our blog post [here](https://about.xethub.com/blog/git-is-for-data-published-in-cidr-2023) for more information on the technical details of this process, including a link to our CIDR 2023 paper.

### Ease of Use

Once a repo is set up to use Git-Xet, nothing more is required. No per-file configuration or tracking is necessary.

### Scalability

Using the `git xet mount` feature, any branch or commit can be mounted locally as a read-only folder.  The data in this folder is seamlessly materialized as needed, allowing large repositories to be viewed quickly.  This is built using our parallel project, [nfsserve](https://github.com/xetdata/nfsserve).

### Flexibility 

Git-Xet naturally integrates seamlessly with [XetHub](https://about.xethub.com/?), which provides a number of nice perks, such as data analytics and integration with tools like [xetcache](https://github.com/xetdata/xetcache) and [pyxet](https://pyxet.readthedocs.io/en/latest/) for easy access to your data from any environment.  It also integrates with GitHub, where repositories can be quickly enabled to use Git-Xet with the [XetData app](https://xethub.com/assets/docs/github-app#xetdata-github-app). Or, just used Git-Xet  locally against a local, private data store.  

### Open Source

Finally, Git-Xet is an open source tool that is free to use and build upon.  By default, the binary data content is stored on XetHub and backed by S3.  It can also be configured against a local data store for a fully free and open source route to managing large repositories.

## Documentation 

Documentation for Git-Xet, PyXet, and the Xet CLI is available on XetHub [here](https://xethub.com/assets/docs/).

## Installation and Development

The easiest way to install Git-Xet is to install one of the prebuilt [binaries](https://github.com/xetdata/xet-tools/releases).  Once Git-Xet is in your path, 

To build Git-Xet from source, [install the rust toolchain](https://doc.rust-lang.org/cargo/getting-started/installation.html), then run `cargo build --release` in the the `rust/` subfolder.  The `git-xet` binary is then in `rust/targets/release/`.  After this, run `git-xet install` to set the proper git config settings.

### Get involved

We will develop this package in public under the BSD license and welcome contributions.

Join our [Slack](https://communityinviter.com/apps/xetdata/xet) to get involved. To stay informed about updates, star this repo and sign up at [XetHub](https://xethub.com/user/sign_up) to get the newsletter.


## Optional: XetHub Account Setup

The use of the fully managed [XetHub](about.xethub.com) service provides many perks, including reliable data storage and seamless minimal-configuration integration with git.

### Authentication

Go to [XetHub](https://xethub.com/user/sign_up) to create an account.  On your first sign-in, you should get a personal access token that can be used for authentication, or go to https://xethub.com/user/settings/pat to create a new token. 

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
