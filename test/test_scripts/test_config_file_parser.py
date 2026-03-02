import os
import configparser
import string
from random import choices
from .config import TEST_CONST
import pytest
from openmsistream.utilities.config_file_parser import ConfigFileParser


@pytest.fixture
def config_objects(logger):
    """Fixture replacing unittest setUp()."""
    cfp = ConfigFileParser(TEST_CONST.TEST_CFG_FILE_PATH, logger=logger)
    parser = configparser.ConfigParser()
    parser.read(TEST_CONST.TEST_CFG_FILE_PATH)
    return cfp, parser


def test_available_group_names(config_objects):
    cfp, parser = config_objects
    assert cfp.available_group_names == parser.sections()


def test_get_config_dict_for_groups(config_objects, monkeypatch, logger, mock_env_vars):
    cfp, parser = config_objects

    # ------ validate minimum sections ----------
    if len(parser.sections()) < 2:
        raise RuntimeError(
            f"ERROR: config file used for testing ({TEST_CONST.TEST_CFG_FILE_PATH}) "
            "does not contain enough sections to test with!"
        )

    # ------ per-group validation ----------
    for group_name in parser.sections():
        group_ref = dict(parser[group_name])
        for k, v in group_ref.items():
            if v.startswith("$"):
                group_ref[k] = os.path.expandvars(v)
        assert cfp.get_config_dict_for_groups(group_name) == group_ref

    # ------ all groups combined ----------
    all_sections_dict = {}
    for group_name in parser.sections():
        all_sections_dict.update(dict(parser[group_name]))
    for k, v in all_sections_dict.items():
        if v.startswith("$"):
            all_sections_dict[k] = os.path.expandvars(v)

    assert cfp.get_config_dict_for_groups(parser.sections()) == all_sections_dict

    # ------ error on nonexistent section ----------
    random_section_name = "".join(choices(string.ascii_letters, k=10))
    while random_section_name in cfp.available_group_names:
        random_section_name = "".join(choices(string.ascii_letters, k=10))

    with pytest.raises(ValueError):
        cfp.get_config_dict_for_groups(random_section_name)

    with pytest.raises(ValueError):
        cfp.get_config_dict_for_groups([parser.sections()[0], random_section_name])

    # ------ Production config handling ----------
    # If env vars are NOT set, fall back to fake config path
    if (
        os.path.expandvars("$KAFKA_PROD_CLUSTER_USERNAME")
        == "$KAFKA_PROD_CLUSTER_USERNAME"
    ):
        chosen_path = TEST_CONST.PROD_CONFIG_FILE_PATH
    else:
        chosen_path = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH

    other_cfp = ConfigFileParser(chosen_path, logger=logger)
    assert "broker" in other_cfp.available_group_names

    with pytest.raises(ValueError):
        other_cfp.get_config_dict_for_groups("broker")
