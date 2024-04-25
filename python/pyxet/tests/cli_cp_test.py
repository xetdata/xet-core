import os
import pytest
import pyxet
import utils
import shutil
import tempfile

from pyxet.file_operations import perform_copy, build_cp_action_list


def test_single_file_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")
    
    try:
        # generate a random file in a temp dir
        dir = tempfile.mkdtemp()
        local_file = f"{dir}/data"
        utils.random_binary_file(local_file, 1024)

        # test variations of path
        source_list = [
            f"{dir}/data",
        ]

        dest_list = [
            # (dest in cp command, expected path of remote file)
            (f"xet://{user}/{repo}/{b1}", [f"xet://{user}/{repo}/{b1}/data"]),
            (f"xet://{user}/{repo}/{b1}/",[f"xet://{user}/{repo}/{b1}/data"]),
            (f"xet://{user}/{repo}/{b1}/zz", [f"xet://{user}/{repo}/{b1}/zz"]),
        ]

        recursive_list = [
            False,
            True,
        ]

        try:
            for src in source_list:
                for dest in dest_list:
                    for r in recursive_list:
                        print(f"xet cp {src} {dest[0]} {r}")
                        perform_copy(src, dest[0], "add data", r)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/*", dest[1])
                        pyxet.PyxetCLI.rm(dest[1])
        finally:
            shutil.rmtree(dir)
    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)

def test_multiple_files_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")

    try:
        # generate random files in a temp dir
        dir = tempfile.mkdtemp()
        local_files = [f"{dir}/data0", f"{dir}/data1"]
        utils.random_binary_files(local_files, [1024, 1024])
            
        # test variations of path
        source_list = [
            f"{dir}/*",
        ]

        dest_list = [
            f"xet://{user}/{repo}/{b1}",
            f"xet://{user}/{repo}/{b1}/",
        ]

        recursive_list = [
            False,
            True,
        ]

        expected_files = [f"xet://{user}/{repo}/{b1}/data0", f"xet://{user}/{repo}/{b1}/data1"]

        try:
            for src in source_list:
                for dest in dest_list:
                    for r in recursive_list:
                        print(f"xet cp {src} {dest} {r}")
                        perform_copy(src, dest, "add data", r)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/*", expected_files)
                        pyxet.PyxetCLI.rm(expected_files)
        finally:
            shutil.rmtree(dir)
    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)

def test_glob_nonrecursive_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")

    try:
        # generate a mix of random files and directories in a temp dir
        dir = tempfile.mkdtemp()
        
        local_files = [f"{dir}/data0", f"{dir}/data1"]
        utils.random_binary_files(local_files, [1024, 1024])

        sub_dir = "subdir"
        os.mkdir(f"{dir}/{sub_dir}")
        utils.random_binary_file(f"{dir}/{sub_dir}/data", 1024)

        # test variations of path
        source_list = [
            f"{dir}/*",
        ]

        dest_list = [
            f"xet://{user}/{repo}/{b1}",
            f"xet://{user}/{repo}/{b1}/",
        ]

        recursive_list = [
            False,
        ]

        expected_files = [f"xet://{user}/{repo}/{b1}/data0", f"xet://{user}/{repo}/{b1}/data1"]

        try:
            for src in source_list:
                for dest in dest_list:
                    for r in recursive_list:
                        print(f"xet cp {src} {dest} {r}")
                        perform_copy(src, dest, "add data", r)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/*", expected_files)
                        utils.assert_remote_files_not_exist(f"xet://{user}/{repo}/{b1}/*", [f"xet://{user}/{repo}/{b1}/{sub_dir}"])
                        pyxet.PyxetCLI.rm(expected_files)
        finally:
            shutil.rmtree(dir)
    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)

def test_glob_recursive_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")

    try:
        # generate a mix of random files and directories in a temp dir
        dir = tempfile.mkdtemp()
        
        local_files = [f"{dir}/data0", f"{dir}/data1"]
        utils.random_binary_files(local_files, [1024, 1024])

        sub_dir = "subdir"
        os.mkdir(f"{dir}/{sub_dir}")
        utils.random_binary_file(f"{dir}/{sub_dir}/data", 1024)
        
        # test variations of path
        source_list = [
            f"{dir}/*",
        ]

        dest_list = [
            f"xet://{user}/{repo}/{b1}",
            f"xet://{user}/{repo}/{b1}/",
        ]

        recursive_list = [
            True,
        ]

        expected_files_level1 = [f"xet://{user}/{repo}/{b1}/data0", f"xet://{user}/{repo}/{b1}/data1", f"xet://{user}/{repo}/{b1}/{sub_dir}"]
        expected_files_level2 = [f"xet://{user}/{repo}/{b1}/{sub_dir}/data"]

        try:
            for src in source_list:
                for dest in dest_list:
                    for r in recursive_list:
                        print(f"xet cp {src} {dest} {r}")
                        perform_copy(src, dest, "add data", r)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/*", expected_files_level1)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/{sub_dir}/*", expected_files_level2)
                        pyxet.PyxetCLI.rm(expected_files_level1)
        finally:
            shutil.rmtree(dir)
    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)
    
def test_directory_nonrecursive_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")

    try:
        # generate a random file in a temp dir
        dir = tempfile.mkdtemp()
        sub_dir = "subdir"
        os.mkdir(f"{dir}/{sub_dir}")
        local_file = f"{dir}/{sub_dir}/data"
        utils.random_binary_file(local_file, 1024)

        # test variations of path
        source_list = [
            (f"{dir}/{sub_dir}/data", True),
            (f"{dir}/{sub_dir}", False),
            (f"{dir}/{sub_dir}/", False)
        ]

        dest_list = [
            f"xet://{user}/{repo}/{b1}",
            f"xet://{user}/{repo}/{b1}/",
        ]

        recursive_list = [
            False,
        ]

        try:
            for src, should_succeed in source_list:
                for dest in dest_list:
                    for r in recursive_list:
                        print(f"xet cp {src} {dest} {r}")
                        
                        if should_succeed:
                            perform_copy(src, dest, "add data", r)
                                
                            utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/*", [f"xet://{user}/{repo}/{b1}/data"])

                        else:

                            # ignores instead of raising error
                            perform_copy(src, dest, "add data", r)

        finally:
            shutil.rmtree(dir)
    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)

def test_directory_recursive_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")
    try:
        # generate a mix of random files and directories in a temp dir
        dir = tempfile.mkdtemp()
        
        local_files = [f"{dir}/data0", f"{dir}/data1"]
        utils.random_binary_files(local_files, [1024, 1024])

        sub_dir = "subdir"
        os.mkdir(f"{dir}/{sub_dir}")
        utils.random_binary_file(f"{dir}/{sub_dir}/data", 1024)

        # test variations of path
        source_list = [
            f"{dir}/*",
        ]

        dest_list = [
            f"xet://{user}/{repo}/{b1}",
            f"xet://{user}/{repo}/{b1}/",
        ]

        recursive_list = [
            True,
        ]

        expected_files_level1 = [f"xet://{user}/{repo}/{b1}/data0", f"xet://{user}/{repo}/{b1}/data1", f"xet://{user}/{repo}/{b1}/{sub_dir}"]
        expected_files_level2 = [f"xet://{user}/{repo}/{b1}/{sub_dir}/data"]

        try:
            for src in source_list:
                for dest in dest_list:
                    for r in recursive_list:
                        print(f"xet cp {src} {dest} {r}")
                        perform_copy(src, dest, "add data", r)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/*", expected_files_level1)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/{sub_dir}/*", expected_files_level2)
                        pyxet.PyxetCLI.rm(expected_files_level1)
                        pyxet.PyxetCLI.rm(expected_files_level2)
        finally:
            shutil.rmtree(dir)
        
    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)

# According to https://filesystem-spec.readthedocs.io/en/latest/copying.html#single-source-to-single-target
# section 1e, if the trailing slash is omitted from "source/subdir" then the subdir is also copied, 
# not just its contents.
#
# NOTE: only use this behavior for the fsspec copy method in python, not the xet cp command line.
def _test_directory_recursive_noslash_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")
    try:
        # generate a mix of random files and directories in a temp dir
        dir = tempfile.mkdtemp()
        dir_name = os.path.basename(dir)
        
        local_files = [f"{dir}/data0", f"{dir}/data1"]
        utils.random_binary_files(local_files, [1024, 1024])

        sub_dir = "subdir"
        os.mkdir(f"{dir}/{sub_dir}")
        utils.random_binary_file(f"{dir}/{sub_dir}/data", 1024)

        # test variations of path
        source_list = [
            f"{dir}",
        ]

        dest_list = [
            f"xet://{user}/{repo}/{b1}",
            f"xet://{user}/{repo}/{b1}/",
        ]

        recursive_list = [
            True,
        ]

        expected_files_level1 = [f"xet://{user}/{repo}/{b1}/{dir_name}"]
        expected_files_level2 = [f"xet://{user}/{repo}/{b1}/{dir_name}/data0", f"xet://{user}/{repo}/{b1}/{dir_name}/data1", f"xet://{user}/{repo}/{b1}/{dir_name}/{sub_dir}"]
        expected_files_level3 = [f"xet://{user}/{repo}/{b1}/{dir_name}/{sub_dir}/data"]

        try:
            for src in source_list:
                for dest in dest_list:
                    for r in recursive_list:
                        print(f"xet cp {src} {dest} {r}")
                        perform_copy(src, dest, "add data", r)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/*", expected_files_level1)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/{dir_name}/*", expected_files_level2)
                        utils.assert_remote_files_exist(f"xet://{user}/{repo}/{b1}/{dir_name}/{sub_dir}/*", expected_files_level3)
                        pyxet.PyxetCLI.rm(expected_files_level1)
        finally:
            shutil.rmtree(dir)
        
    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)

def test_large_batch_upload():
    user = utils.test_account_login()
    repo = utils.test_repo()
    b1 = utils.new_random_branch_from(f"xet://{user}/{repo}", "main")

    try:
        # generate a large batch of random files in a temp dir
        dir = tempfile.mkdtemp()
        
        n_files = 1000
        local_files = list(map(lambda i: f"{dir}/data{i}", range(n_files)))
        utils.random_binary_files(local_files, [1024] * n_files)

        try:
            pyxet.commit_transaction.TRANSACTION_FILE_LIMIT = 100
            perform_copy(f"{dir}/", f"xet://{user}/{repo}/{b1}", "add data", True)
        finally:
            shutil.rmtree(dir)

    finally:
        pyxet.BranchCLI.delete(f"xet://{user}/{repo}", b1, True)

def test_size_hint():
    user = utils.test_account_login()
    repo = utils.test_repo()

    try:
        # generate a large batch of random files in a temp dir
        dir = tempfile.mkdtemp()
        local_files = [f"{dir}/data0", f"{dir}/data1"]
        utils.random_binary_files(local_files, [262, 471])

        cplist = build_cp_action_list(f"{dir}/*", f"xet://{user}/{repo}/main")
        assert len(cplist) == 2
        assert cplist[0].size == 262
        assert cplist[1].size == 471

    finally:
        shutil.rmtree(dir)