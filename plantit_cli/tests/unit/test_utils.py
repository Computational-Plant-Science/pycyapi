from plantit_cli.utils import parse_docker_image_components, docker_image_exists


def test_parse_docker_image_components():
    full = "docker://computationalplantscience/3d-model-reconstruction:cuda"
    owner, name, tag = parse_docker_image_components(full)
    assert owner == "computationalplantscience"
    assert name == "3d-model-reconstruction"
    assert tag == "cuda"


def test_docker_image_exists_returns_true_when_does_exist():
    full = "docker://computationalplantscience/3d-model-reconstruction:cuda"
    owner, name, tag = parse_docker_image_components(full)
    assert docker_image_exists(name, owner, tag)


def test_docker_image_exists_returns_false_when_does_not_exist():
    full = "docker://computationalplantscience/doesnt-exist"
    owner, name, tag = parse_docker_image_components(full)
    assert not docker_image_exists(name, owner, tag)