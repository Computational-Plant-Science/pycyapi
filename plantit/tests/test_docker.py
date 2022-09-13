from plantit.docker import parse_image_components


def test_parse_image_components_no_owner_or_tag():
    image = "alpine"
    owner, name, tag = parse_image_components(image)
    assert not owner
    assert not tag
    assert name == "alpine"


def test_parse_image_components_no_owner():
    image = "ubuntu:xenial"
    owner, name, tag = parse_image_components(image)
    assert not owner
    assert tag == "xenial"
    assert name == "ubuntu"


def test_parse_image_components():
    image = "computationalplantscience/plantit:latest"
    owner, name, tag = parse_image_components(image)
    assert owner == "computationalplantscience"
    assert name == "plantit"
    assert tag == "latest"
