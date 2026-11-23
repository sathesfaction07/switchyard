from __future__ import annotations

import pytest

from parallel_ops.core.schema import (
    SCHEMAS,
    CatalogRecord,
    StatusRecord,
    get_schema,
    validate_payload,
)
from parallel_ops.resilience.errors import SchemaError


class TestGetSchema:
    @pytest.mark.parametrize("name", ["catalog", "CATALOG", " Catalog "])
    def test_resolves_case_and_whitespace_insensitively(self, name):
        assert get_schema(name) is CatalogRecord

    @pytest.mark.parametrize("name", ["none", "off", "", "  ", None])
    def test_validation_can_be_switched_off(self, name):
        """
        Needed for a first run against an unfamiliar endpoint: you want to see what
        actually comes back before committing to a contract.
        """
        assert get_schema(name) is None

    def test_an_unknown_name_lists_what_is_available(self):
        with pytest.raises(ValueError, match="unknown schema 'catalogue'"):
            get_schema("catalogue")

    def test_the_error_names_the_known_schemas(self):
        with pytest.raises(ValueError) as caught:
            get_schema("nope")
        message = str(caught.value)
        for known in SCHEMAS:
            assert known in message
        assert "none" in message


class TestValidatePayload:
    def test_no_model_passes_the_payload_straight_through(self):
        payload = {"anything": "at all", "nested": {"fine": True}}
        assert validate_payload(payload, None) is payload

    def test_a_good_payload_comes_back_normalised(self):
        result = validate_payload(
            {"product_id": "SKU-1", "status": "active", "region": "us"}, CatalogRecord
        )
        assert result["product_id"] == "SKU-1"
        assert result["updated_at"] == ""

    def test_unknown_fields_are_ignored_not_rejected(self):
        """
        Upstream adding a field is not a reason to fail a batch. A strict model turns
        every harmless additive change into an outage on our side.
        """
        result = validate_payload(
            {
                "product_id": "SKU-1",
                "status": "active",
                "region": "us",
                "brand_new_field": "added last tuesday",
            },
            CatalogRecord,
        )
        assert result["product_id"] == "SKU-1"
        assert "brand_new_field" not in result

    def test_a_missing_field_names_the_field(self):
        with pytest.raises(SchemaError, match="region"):
            validate_payload({"product_id": "SKU-1", "status": "active"}, CatalogRecord)

    def test_a_bad_enum_value_says_what_was_expected(self):
        """
        The field-level detail is the entire value of validating. "3 items invalid" is
        not actionable; "status: Input should be 'active'..." tells you immediately that
        upstream started returning uppercase.
        """
        with pytest.raises(SchemaError) as caught:
            validate_payload(
                {"product_id": "SKU-1", "status": "ACTIVE", "region": "us"}, CatalogRecord
            )

        message = str(caught.value)
        assert "status" in message
        assert "active" in message

    def test_the_model_name_is_in_the_message(self):
        with pytest.raises(SchemaError, match="CatalogRecord"):
            validate_payload({}, CatalogRecord)

    def test_an_empty_string_id_is_rejected(self):
        """A blank identifier is worse than a missing one; it looks valid downstream."""
        with pytest.raises(SchemaError, match="product_id"):
            validate_payload({"product_id": "", "status": "active", "region": "us"}, CatalogRecord)

    def test_all_three_missing_fields_are_reported_without_truncation(self):
        """Three required fields is exactly the truncation limit, so nothing is elided."""
        with pytest.raises(SchemaError) as caught:
            validate_payload({}, CatalogRecord)

        message = str(caught.value)
        for field in ("product_id", "status", "region"):
            assert field in message
        assert "more" not in message

    def test_more_errors_than_the_limit_are_truncated_with_a_count(self):
        """
        A wholly unexpected payload can produce a dozen errors and the first few already
        tell you what happened, so the tail is summarised rather than printed.
        """
        with pytest.raises(SchemaError) as caught:
            validate_payload(
                {"product_id": [], "status": [], "region": {}, "updated_at": []},
                CatalogRecord,
            )

        assert "and 1 more" in str(caught.value)

    def test_status_record_only_needs_a_verdict(self):
        result = validate_payload({"product_id": "SKU-1", "status": "whatever"}, StatusRecord)
        assert result == {"product_id": "SKU-1", "status": "whatever"}

    def test_status_record_does_not_constrain_the_vocabulary(self):
        """
        A bare status probe is used against endpoints whose vocabulary we do not control,
        so constraining it would make the looser schema useless.
        """
        result = validate_payload({"product_id": "1", "status": "Aktiv"}, StatusRecord)
        assert result["status"] == "Aktiv"


class TestShippedSchemas:
    def test_every_registered_schema_is_resolvable(self):
        for name in SCHEMAS:
            assert get_schema(name) is not None

    @pytest.mark.parametrize("name", sorted(SCHEMAS))
    def test_every_registered_schema_ignores_extras(self, name):
        """
        Consistency matters here: a strict schema hiding among the lenient ones would
        fail batches for a reason nobody would look for.
        """
        model = SCHEMAS[name]
        assert model.model_config.get("extra") == "ignore"

    def test_the_default_configured_schema_exists(self):
        from parallel_ops.config import Settings

        assert get_schema(Settings().schema_name) is not None
