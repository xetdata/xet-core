# Allows pushing to this repo.
git config --global receive.denyCurrentBranch ignore
git config --global push.autoSetupRemote true

# Make sure the filter is configured globally.
git xet install 

base_dir="`pwd`"
mkdir config_files/
