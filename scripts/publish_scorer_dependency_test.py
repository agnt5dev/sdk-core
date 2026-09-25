import io
import json
import runpy
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).with_name("publish-scorer-dependency.py")

class PublishScorerDependencyTest(unittest.TestCase):
    def test_existing_release_is_not_republished(self):
        response = io.BytesIO(json.dumps({"version":{"num":"0.1.0","yanked":False}}).encode())
        with patch("urllib.request.urlopen", return_value=response), patch("subprocess.run") as publish:
            runpy.run_path(str(SCRIPT))
            publish.assert_not_called()

    def test_only_not_found_permits_publication(self):
        for code in [404, 403, 429, 500]:
            with self.subTest(code=code), patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError("url",code,"failure",{},None)), patch("subprocess.run") as publish:
                if code == 404:
                    runpy.run_path(str(SCRIPT))
                    publish.assert_called_once_with(["cargo","publish","-p","agnt5-eval-scorers","--locked"],check=True)
                else:
                    with self.assertRaises(urllib.error.HTTPError):
                        runpy.run_path(str(SCRIPT))
                    publish.assert_not_called()

    def test_yanked_release_fails_closed(self):
        response = io.BytesIO(json.dumps({"version":{"num":"0.1.0","yanked":True}}).encode())
        with patch("urllib.request.urlopen", return_value=response), patch("subprocess.run") as publish:
            with self.assertRaises(RuntimeError):
                runpy.run_path(str(SCRIPT))
            publish.assert_not_called()
