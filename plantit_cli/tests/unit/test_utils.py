from plantit_cli.utils import parse_docker_image_components, docker_image_exists


def test_parse_docker_image_components():
    full = "docker://computationalplantscience/plantit"
    owner, name, tag = parse_docker_image_components(full)
    assert owner == "computationalplantscience"
    assert name == "plantit"
    # assert tag == "latest"


def test_docker_image_exists_returns_true_when_does_exist():
    full = "docker://computationalplantscience/plantit"
    owner, name, tag = parse_docker_image_components(full)
    assert docker_image_exists(name, owner, tag)


def test_docker_image_exists_returns_false_when_does_not_exist():
    full = "docker://computationalplantscience/dne"
    owner, name, tag = parse_docker_image_components(full)
    assert not docker_image_exists(name, owner, tag)