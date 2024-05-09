"""
Provides URL parsing for Xet Repos
"""
import unittest
from urllib.parse import urlparse
import sys

class XetPathInfo:
    __slots__ = ['scheme', 'domain', 'user', 'token', 'repo', 'branch', 'path']

    def _user_at_domain(self):
        if self.token: 
            user = f"{self.user}:{self.token}"
        else:
            user = self.user

        return f'{user}@{self.domain}'
    
    def _domain_sl_user(self):
        if self.token: 
            user = f"{self.user}:{self.token}"
        else:
            user = self.user

        return f'{self.domain}/{user}'

    def _repo_branch_path(self):
        return "/".join(s for s in [self.repo, self.branch, self.path] if s)

    def url(self):
        return f"{self.scheme}://{self._user_at_domain()}/{self._repo_branch_path()}"

    def remote(self, branch = False):
        """
        Returns the endpoint of this in the qualified user[:token]@domain
        """
        
        if branch and self.branch:
            ret = f"https://{self._domain_sl_user()}/{self.repo}/{self.branch}"
        elif self.repo:
            ret = f"https://{self._domain_sl_user()}/{self.repo}"
        else:
            ret = f"https://{self._domain_sl_user()}"

        # This should work but has issues in xet-core
        #if branch and self.branch:
        #    ret = f"https://{self._user_at_domain()}/{self.repo}/{self.branch}"
        #elif self.repo:
        #    ret = f"https://{self._user_at_domain()}/{self.repo}"
        #else:
        #    ret = f"https://{self._user_at_domain()}"
        
        print(f"remote() = {ret}")
        return ret
    
    def domain_url(self):
        """
        https://user@domain/
        """
        return f"https://{self._user_at_domain()}"

    def name(self):
        """
        Returns the prefix: user/repo/branch/path
        """
        return f"{self.user}/{self._repo_branch_path()}"

    def __eq__(self, other: object) -> bool:
        return (self.scheme == other.scheme
                and self.domain == other.domain
                and self.user == other.user
                and self.token == other.token
                and self.repo == other.repo
                and self.branch == other.branch
                and self.path == other.path)

    def __repr__(self):
        return self.url()


has_warned_user_on_url_format = False

def parse_url(url, default_domain='xethub.com', expect_branch = None, expect_repo = True):
    """
    Parses a Xet URL of the form 
     - xet://user/repo/branch/[path]
     - /user/repo/branch/[path]

    Into a XetPathInfo which forms it as remote=https://[domain]/user/repo
    branch=[branch] and path=[path].

    branches with '/' are not supported.

    If partial_remote==True, allows [repo] to be optional. i.e. it will
    parse /user or xet://user
    """

    url = url.lstrip('/')
    parse = urlparse(url)
    if parse.scheme == '':
        parse = parse._replace(scheme='xet')

    # support default_domain with a scheme (http/https)
    domain_split = default_domain.split('://')
    scheme = 'https'
    if len(domain_split) == 2:
        scheme = domain_split[0]
        default_domain = domain_split[1]

    # Set this as a default below     

    ret = XetPathInfo()
    # Set what defaults we can
    ret.domain = default_domain
    ret.scheme = "xet"
    ret.token = None

    # Handle the case where we are xet://user/repo. In which case the domain
    # parsed is not xethub.com and domain="user".
    # we rewrite the parse the handle this case early.
    if "@" not in parse.netloc:
        global has_warned_user_on_url_format
        
        if not has_warned_user_on_url_format:
            sys.stderr.write("Warning:  The use of the xet:// prefix without an endpoint is deprecated and will be disabled in the future.\n"
                            f"          Please switch URLs to use the format xet://<user>@<endpoint>/<repo>/<branch>/<path>.\n"
                            f"          Endpoint now defaulting to {default_domain}.\n\n")
            has_warned_user_on_url_format = True

        if parse.netloc.endswith(".com"):  # Cheap way now to see if it's a website or not; we won't hit this with the new format.
            ret.domain = parse.netloc
            path_to_parse = parse.path
        else:
            ret.domain = default_domain
            path_to_parse = f"{parse.netloc}/{parse.path}"
    else:
        
        # Hack until we can clean up this parsing logic
        user_at_domain = parse.netloc.split("@")
        if len(user_at_domain) == 2:
            user, ret.domain = user_at_domain
            path_to_parse = f"{user}/{parse.path}"
        else: 
            raise ValueError(f"Cannot parse user and endpoint from {parse.netloc}")


    path_endswith_slash = path_to_parse.endswith("/")

    components = list([t for t in [t.strip() for t in path_to_parse.split('/')] if t])

    if len(components) == 0:
        raise ValueError(f"Invalid Xet URL format; user must be specified. Expecting xet://user@domain/[repo/[branch[/path]]], got {url}")
    
    if len(components) == 1 and expect_repo is True:
        raise ValueError(f"Invalid Xet URL format; user and repo must be specified. Expecting xet://user@domain/repo/[branch[/path]], got {url}")
    
    if len(components) > 1 and expect_repo is False:
        raise ValueError(f"Invalid Xet URL format for user; repo given.  Expecting xet://user@domain/, got {url}")
       
    if len(components) == 2 and expect_branch is True:
        raise ValueError(f"Invalid Xet URL format; user, repo, and branch must be specified for this operation. Expecting xet://user@domain/repo/branch[/path], got {url}")

    if len(components) > 2 and expect_branch is False:
        raise ValueError(f"Invalid Xet URL format for repo; branch given.  Expecting xet://user@domain/repo, got {url}")

    user_info = components[0]
    if ":" in user_info: 
        ret.user, ret.token = user_info.split(":")
    else:
        ret.user = user_info

    if len(components) > 1:
        ret.repo = components[1]
    else:
        ret.repo = ""
    
    if len(components) > 2:
        ret.branch = components[2]
    else:
        ret.branch = ""

    if len(components) > 3:
        ret.path = "/".join(components[3:])
        if path_endswith_slash:
            ret.path = ret.path + "/"
    else:
        ret.path = ""

    return ret

class ParseUrlTest(unittest.TestCase):

    def parse_url(self, url, expect_warning, **kwargs):
        global has_warned_user_on_url_format
        has_warned_user_on_url_format = False
        parse = parse_url(url, **kwargs)
        self.assertEqual(has_warned_user_on_url_format, expect_warning)
        print(f"Test parse result = {parse}")

        self.assertEqual(parse, parse_url(parse.url()))
        return parse

    def test_parse_xet_url(self):
        parse = self.parse_url("xet://xethub.com/user/repo/branch/hello/world", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = self.parse_url("xet://xethub.com/user/repo/branch/hello/world/", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = self.parse_url("xet://xethub.com/user/repo/branch/", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://xethub.com/user/repo/branch", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://xethub.com/user/repo", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://xethub.com/user/repo/branch", True, default_domain='xetbeta.com')
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        with self.assertRaises(ValueError):
            self.parse = self.parse_url("xet://xethub.com/user", True)

    def test_parse_xet_url_truncated(self):
        parse = self.parse_url("xet://user/repo/branch/hello/world", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = self.parse_url("xet://user/repo/branch/hello/world/", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = self.parse_url("xet://user/repo/branch/", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://user/repo/branch", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://user/repo", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://user/repo/branch", True, default_domain='xetbeta.com')
        self.assertEqual(parse.remote(), "https://user@xetbeta.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        with self.assertRaises(ValueError):
            self.parse = self.parse_url("xet://user", True)

    def test_parse_plain_path(self):
        parse = self.parse_url("/user/repo/branch/hello/world", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = self.parse_url("/user/repo/branch/hello/world/", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = self.parse_url("/user/repo/branch/", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("/user/repo/branch", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("/user/repo", True)
        self.assertEqual(parse.remote(), "https://user@xethub.com/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("/user/repo/branch", True, default_domain='xetbeta.com')
        self.assertEqual(parse.remote(), "https://user@xetbeta.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        with self.assertRaises(ValueError):
            self.parse = self.parse_url("xet://xethub.com/user", True)
    
    def test_parse_xet_url_correct(self):
        parse = self.parse_url("xet://user@xh.com/repo/branch/hello/world", False)
        self.assertEqual(parse.remote(), "https://user@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = self.parse_url("xet://user@xh.com/repo/branch/hello/world/", False)
        self.assertEqual(parse.remote(), "https://user@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = self.parse_url("xet://user@xh.com/repo/branch/", False)
        self.assertEqual(parse.remote(), "https://user@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://user@xh.com/repo/branch", False)
        self.assertEqual(parse.remote(), "https://user@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://user@xh.com/repo", False)
        self.assertEqual(parse.remote(), "https://user@xh.com/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://XetHub@hub.xetsvc.com/Flickr30k/main", False)
        self.assertEqual(parse.remote(), "https://XetHub@hub.xetsvc.com/Flickr30k")
        self.assertEqual(parse.branch, "main")
        self.assertEqual(parse.path, "")


        parse = self.parse_url("xet://user@xh.com/repo/branch", False, default_domain='xetbeta.com')
        self.assertEqual(parse.remote(), "https://user@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")
    
    def test_parse_xet_url_correct_with_token(self):
        parse = self.parse_url("xet://user:token@xh.com/repo/branch/hello/world", False)
        self.assertEqual(parse.remote(), "https://user:token@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = self.parse_url("xet://user:token@xh.com/repo/branch/hello/world/", False)
        self.assertEqual(parse.remote(), "https://user:token@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = self.parse_url("xet://user:token@xh.com/repo/branch/", False)
        self.assertEqual(parse.remote(), "https://user:token@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://user:token@xh.com/repo/branch", False)
        self.assertEqual(parse.remote(), "https://user:token@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://user:token@xh.com/repo", False)
        self.assertEqual(parse.remote(), "https://user:token@xh.com/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = self.parse_url("xet://XetHub:token@hub.xetsvc.com/Flickr30k/main", False)
        self.assertEqual(parse.remote(), "https://XetHub:token@hub.xetsvc.com/Flickr30k")
        self.assertEqual(parse.branch, "main")
        self.assertEqual(parse.path, "")


        parse = self.parse_url("xet://user:token@xh.com/repo/branch", False, default_domain='xetbeta.com')
        self.assertEqual(parse.remote(), "https://user:token@xh.com/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")
