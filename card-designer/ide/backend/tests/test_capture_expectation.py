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
            + words("PERSONAL NUMBER", "000123"),
            words("DATE OF BIRTH", "06/11/1990")
            + words("ISSUING COUNTRY", "Mexico")
            + words("EXPIRES", "06/11/2040", clipped=True),
        ])
        self.assertEqual(expected, {
            "full_name": "Ana María López", "given_names": "Ana María", "surnames": "López",
            "birth_date": "06/11/1990", "document_number": "000042", "issued_identifier": "000123",
            "address": "", "issue_date": "", "expiry_date": "", "issuing_country": "Mexico",
        })

    def test_identifier_accepts_new_and_legacy_layout_annotations(self):
        for field, metadata in [
            ("issued_identifier", {}),
            ("personal_number", {}),
            (None, {"capture_field": "issued_identifier"}),
            (None, {"capture_field": "personal_number"}),
        ]:
            with self.subTest(field=field, metadata=metadata):
                expected = capture_expectation([words(field, "ABC0123", **metadata)])
                self.assertEqual(expected["issued_identifier"], "ABC0123")
                self.assertNotIn("personal_number", expected)

    def test_full_name_uses_structured_parts_only_when_they_match_printed_name(self):
        identity = {"given_names": "Silas Grey", "family_name": "Bennett"}
        expected = capture_expectation([words("NAME", "Silas", "Grey", "Bennett")], [identity])
        self.assertEqual((expected["given_names"], expected["surnames"]), ("Silas Grey", "Bennett"))
        expected = capture_expectation([words("NAME", "Someone", "Else")], [identity])
        self.assertEqual((expected["given_names"], expected["surnames"]), ("", ""))

    def test_repeated_values_do_not_duplicate_and_conflicting_values_are_empty(self):
        self.assertEqual(capture_expectation([
            words("DOB", "01/01/2000") + words("NAME", "Ada"),
            words("DATE OF BIRTH", "01/01/2000") + words("FULL NAME", "Grace"),
        ])["birth_date"], "01/01/2000")
        self.assertEqual(capture_expectation([words("NAME", "Ada"), words("NAME", "Grace")])["full_name"], "")
        self.assertIn("expected-card-capture.json", expected_files([{"stem": "front"}]))
