"""
Provides URL parsing for Xet Repos
"""
import unittest
from collections import namedtuple
from urllib.parse import urlparse

XetPathInfo = namedtuple('XetPathInfo', ('remote', 'branch', 'path'))


def parse_url(url, force_domain='xethub.com', partial_remote=False):
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

    # support force_domain with a scheme (http/https)
    domain_split = force_domain.split('://')
    scheme = 'https'
    if len(domain_split) == 2:
        scheme = domain_split[0]
        force_domain = domain_split[1]

    if parse.scheme != 'xet':
        raise ValueError('Invalid protocol')

    # Handle the case where we are xet://user/repo. In which case the domain
    # parsed is not xethub.com and domain="user".
    # we rewrite the parse the handle this case early.
    if parse.netloc != force_domain:
        if parse.netloc == 'xethub.com':
            parse = parse._replace(netloc=force_domain)
        else:
            # this is of the for xet://user/repo/...
            # join user back with path
            newpath = f"/{parse.netloc}{parse.path}"
            # replace the netloc
            true_netloc = force_domain
            parse = parse._replace(netloc=true_netloc, path=newpath)

    # Split the known path and try to split out the user/repo/branch/path components
    path = parse.path
    components = path.split('/')
    # path always begin with a '/', so 1st component is always empty
    # so the minimum for a remote is xethub.com/user/repo
    if len(components) < 3:
        # <=2 components. Note that 1st component is always empty 
        if partial_remote and len(components) == 2:
            replacement_parse_path = '/'.join(components)
            parse = parse._replace(path=replacement_parse_path, scheme=scheme)
            return XetPathInfo(parse.geturl(), "", "")
        raise ValueError("Invalid Xet URL format: Expecting xet://user/repo/[branch]/[path]")

    branch = ""
    if len(components) >= 4:
        branch = components[3]

    path = ""
    if len(components) >= 5:
        path = '/'.join(components[4:])

    # we leave url with the first 3 components. i.e. "/user/repo"
    replacement_parse_path = '/'.join(components[:3])
    parse = parse._replace(path=replacement_parse_path, scheme=scheme)

    return XetPathInfo(parse.geturl(), branch, path)


class ParseUrlTest(unittest.TestCase):
    def test_parse_xet_url(self):
        parse = parse_url("xet://xethub.com/user/repo/branch/hello/world")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = parse_url("xet://xethub.com/user/repo/branch/hello/world/")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = parse_url("xet://xethub.com/user/repo/branch/")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = parse_url("xet://xethub.com/user/repo/branch")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = parse_url("xet://xethub.com/user/repo")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = parse_url("xet://xethub.com/user/repo/branch", force_domain='xetbeta.com')
        self.assertEqual(parse.remote, "https://xetbeta.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        with self.assertRaises(ValueError):
            self.parse = parse_url("xet://xethub.com/user")

    def test_parse_xet_url_truncated(self):
        parse = parse_url("xet://user/repo/branch/hello/world")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = parse_url("xet://user/repo/branch/hello/world/")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = parse_url("xet://user/repo/branch/")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = parse_url("xet://user/repo/branch")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = parse_url("xet://user/repo")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = parse_url("xet://user/repo/branch", force_domain='xetbeta.com')
        self.assertEqual(parse.remote, "https://xetbeta.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        with self.assertRaises(ValueError):
            self.parse = parse_url("xet://user")

    def test_parse_plain_path(self):
        parse = parse_url("/user/repo/branch/hello/world")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world")

        parse = parse_url("/user/repo/branch/hello/world/")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "hello/world/")

        parse = parse_url("/user/repo/branch/")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = parse_url("/user/repo/branch")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        parse = parse_url("/user/repo")
        self.assertEqual(parse.remote, "https://xethub.com/user/repo")
        self.assertEqual(parse.branch, "")
        self.assertEqual(parse.path, "")

        parse = parse_url("/user/repo/branch", force_domain='xetbeta.com')
        self.assertEqual(parse.remote, "https://xetbeta.com/user/repo")
        self.assertEqual(parse.branch, "branch")
        self.assertEqual(parse.path, "")

        with self.assertRaises(ValueError):
            self.parse = parse_url("xet://xethub.com/user")
