#[cfg(test)]
mod git_repo_tests_2 {
    use gitxetcore::errors::Result;
    use gitxetcore::git_integration::git_repo::test_tools::TestRepo;
    use std::path::Path;

    fn setup_path() {
        let git_xet_path = env!("CARGO_BIN_EXE_git-xet");
        let path = Path::new(&git_xet_path).parent().unwrap();
        let full_path = std::env::var("PATH").unwrap();
        std::env::set_var("PATH", format!("{}:{}", path.to_str().unwrap(), &full_path));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_new_remotes_are_detected() -> Result<()> {
        // Add git-xet to the path.
        setup_path();
        let tr_origin = TestRepo::new()?;
        let tr_upstream = TestRepo::new()?;
        let mut tr = TestRepo::clone(&tr_origin)?;

        let (has_changed, things_to_commit) = tr.repo.initialize(true, false).await?;
        assert!(has_changed);
        assert!(things_to_commit);

        assert_eq!(tr.repo.current_remotes()?.len(), 1);

        tr.repo
            .run_git_checked_in_repo("add", &[".gitattributes"])?;
        tr.repo
            .run_git_checked_in_repo("commit", &["-a", "-m", "Initial"])?;

        let (has_changed, things_to_commit) = tr.repo.initialize(true, false).await?;

        assert!(!has_changed);
        assert!(!things_to_commit);

        tr.repo.run_git_checked_in_repo(
            "remote",
            &[
                "add",
                "upstream",
                tr_upstream.repo.repo_dir.to_str().unwrap(),
            ],
        )?;

        let (has_changed, things_to_commit) = tr.repo.initialize(true, false).await?;

        assert!(has_changed);
        assert!(!things_to_commit);

        assert_eq!(tr.repo.current_remotes()?.len(), 2);

        Ok(())
    }
}
