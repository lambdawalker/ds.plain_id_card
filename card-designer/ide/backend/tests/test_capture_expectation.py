import unittest

from backend.export_files import capture_expectation, expected_files


def words(field, *values, **metadata):
    return [dict(field=field, role="value", text=value, **metadata) for value in values]


class CaptureExpectationTest(unittest.TestCase):
    def test_matches_form_contract_using_visible_values_from_both_sides(self):
        expected = capture_expectation([
            words("NAME", "Ana", "María", capture_field="given_names")
            + words("NAME", "López", capture_field="surnames")
            + words("DOCUMENT NO.", "000042")
            + words("NATIONALITY", "Mexican")
            + words("PERSONAL NUMBER", "—"),
            words("DATE OF BIRTH", "06/11/1990")
            + words("ISSUING COUNTRY", "Mexico")
            + words("EXPIRES", "06/11/2040", clipped=True),
        ])
        self.assertEqual(expected, {
            "full_name": "Ana María López", "given_names": "Ana María", "surnames": "López",
            "birth_date": "06/11/1990", "document_number": "000042", "personal_number": "",
            "address": "", "issue_date": "", "expiry_date": "", "issuing_country": "Mexico",
        })

    def test_repeated_values_do_not_duplicate_and_conflicting_values_are_empty(self):
        self.assertEqual(capture_expectation([
            words("DOB", "01/01/2000") + words("NAME", "Ada"),
            words("DATE OF BIRTH", "01/01/2000") + words("FULL NAME", "Grace"),
        ])["birth_date"], "01/01/2000")
        self.assertEqual(capture_expectation([words("NAME", "Ada"), words("NAME", "Grace")])["full_name"], "")
        self.assertIn("expected-card-capture.json", expected_files([{"stem": "front"}]))
