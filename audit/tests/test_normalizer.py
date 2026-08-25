"""Normalizer fixtures — synthetic names only (known parser traps + entity)."""

from __future__ import annotations

import unittest

from audit.normalizer import (
    detect_truncation,
    fold_accents,
    keys_with_transposition,
    normalize_drake_client_name,
    normalize_entity,
    normalize_log_row,
    surname_variants,
    transposed_interpretation,
)


class NormalizerFixtureTests(unittest.TestCase):
    def test_accent_and_n_folding(self):
        self.assertEqual(fold_accents("Pérez"), "Perez")
        self.assertEqual(fold_accents("Muñoz"), "Munoz")
        self.assertEqual(fold_accents("señor"), "senor")

    def test_spanish_particles(self):
        n = normalize_drake_client_name("DE LA CRUZ GARCIA, JUAN")
        self.assertIn("DE LA CRUZ GARCIA", n.surname_variants)
        self.assertIn("DE LA CRUZ", n.surname_variants)
        self.assertEqual(n.first_key, "JUAN")

    def test_maternal_surname_variant(self):
        variants = surname_variants(["BOCANEGRA", "GALLEGOS"])
        self.assertEqual(variants[0], "BOCANEGRA GALLEGOS")
        self.assertEqual(variants[1], "BOCANEGRA")

    def test_trap_y_is_middle_initial_not_conjunction(self):
        n = normalize_drake_client_name(
            "ABU TAHA, ZAID AHMED Y & GIULIANA CASSIN"
        )
        self.assertTrue(n.is_joint)
        self.assertEqual(n.spouse_chunk.upper().split()[0], "GIULIANA")
        self.assertEqual(n.first_key, "ZAID")

    def test_trap_missing_spaces_around_ampersand(self):
        n = normalize_drake_client_name("GOMEZ, MIGUEL& LOURDES")
        self.assertTrue(n.is_joint)
        self.assertEqual(n.first_key, "MIGUEL")
        self.assertTrue(n.spouse_chunk.upper().startswith("LOURDES"))

        n2 = normalize_drake_client_name("RAMIREZ, FEDERICO & MARIBELMUNOZ")
        self.assertTrue(n2.is_joint)
        self.assertIn("MARIBELMUNOZ", n2.spouse_chunk.upper().replace(" ", ""))

    def test_trap_trailing_letter_on_surname_is_initial(self):
        n = normalize_drake_client_name("AGUILAR FLORES G, MONICA")
        self.assertEqual(n.surname_full, "AGUILAR FLORES")
        self.assertNotIn("G", n.surname_full.split())
        self.assertEqual(n.first_key, "MONICA")

    def test_trap_column_contamination_transposition(self):
        keys = keys_with_transposition("AGUIRRE", "JIMENEZ")
        surs = {k[0] for k in keys}
        self.assertIn("AGUIRRE", surs)
        t = transposed_interpretation("AGUIRRE", "JIMENEZ")
        self.assertEqual(t.surname_full, "JIMENEZ")
        d = normalize_drake_client_name("AGUIRRE JIMENEZ, ANTONIO")
        self.assertIn("AGUIRRE JIMENEZ", d.surname_variants)
        self.assertIn("AGUIRRE", d.surname_variants)

    def test_trap_middle_initial_conflict_first_token_stable(self):
        a = normalize_log_row("GARCIA", "ROBERTA A")
        b = normalize_drake_client_name("GARCIA, ROBERTA G")
        self.assertEqual(a.first_key, "ROBERTA")
        self.assertEqual(b.first_key, "ROBERTA")
        self.assertEqual(a.match_keys[0], b.match_keys[0])

    def test_truncation_flag_at_39(self):
        raw = "X" * 39
        trunc, unreliable = detect_truncation(raw)
        self.assertTrue(trunc)
        self.assertTrue(unreliable)
        long_name = "PEREZ RODRIGUEZ MARTINEZ, JOSE ANTONIO & MAR"
        self.assertGreaterEqual(len(long_name), 39)
        n2 = normalize_drake_client_name(long_name[:40])
        self.assertTrue(n2.truncated)


class EntityNormalizerTests(unittest.TestCase):
    def test_ampersand_kept_not_spouse_split(self):
        e = normalize_entity("J & J FENCE AND CONSTRUCTION INC")
        self.assertIn("&", e.primary)
        e2 = normalize_entity("J&H MECHANICAL INC")
        self.assertIn("&", e2.primary)

    def test_strip_trailing_inc_llc_corp(self):
        self.assertEqual(normalize_entity("ACME SERVICES INC").primary, "ACME SERVICES")
        self.assertEqual(normalize_entity("ACME SERVICES LLC").primary, "ACME SERVICES")
        self.assertEqual(
            normalize_entity("ACME SERVICES CORPORATION").primary, "ACME SERVICES"
        )
        self.assertEqual(normalize_entity("ACME SERVICES L.L.C.").primary, "ACME SERVICES")

    def test_co_only_trailing(self):
        self.assertEqual(
            normalize_entity("LIMITLESS AUTOMATION CO").primary, "LIMITLESS AUTOMATION"
        )
        e = normalize_entity("CO OP MARKET INC")
        self.assertTrue(e.primary.startswith("CO"))

    def test_leading_the(self):
        e = normalize_entity("THE PRINCESS MATTRESS INC")
        self.assertEqual(e.primary, "PRINCESS MATTRESS")
        self.assertIn("stripped_the", e.notes)

    def test_digits_hyphen_and_leading_zero(self):
        e = normalize_entity("1441-45 EL SEGUNDO LLC")
        self.assertIn("1441", e.primary)
        self.assertIn("45", e.primary)
        e2 = normalize_entity("011 INTERNATIONAL LLC")
        self.assertTrue(any(k.startswith("011") or k.startswith("11") for k in e2.keys))

    def test_dba_indexes_both_sides(self):
        e = normalize_entity("ALPHA LLC DBA BETA TRADING")
        self.assertEqual(e.primary, "ALPHA")
        self.assertTrue(any("BETA" in k for k in e.keys))


if __name__ == "__main__":
    unittest.main()
