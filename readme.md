# Install

    sudo apt-get install gcc python3-dev openssl libmariadb3 libmariadb-dev
    git clone https://github.com/mariadb-corporation/mariadb-connector-python/
    cd mariadb-connector-python
    git checkout remotes/origin/1.0
    python3 setup.py build
    python3 -m pip install .
    python3 -m pip install hyperopt

# Configuration

Add server IP and address to `~/.bashrc`

    export CRAVE_RESULTS_HOST=localhost
    export CRAVE_RESULTS_PORT=65099

# Usage

Add crave results as submodule

    git submodule add <repo>

Usage example

    from CraveResults import CraveResults, CraveResultsException

    db = CraveResults()

    config = dict(
        learning_rate=0.01,
        momentum=0.2,
        architecture="CNN",
        dataset_id="peds-0192",
        infra="AWS",
    )

    db.init({
        "project": "detect_pedestrians",
        "notes": "tweak baseline",
        "tags": ["baseline", "paper1"],
        "config": config
    })
    
    for epoch in epochs:
        db.log(
            {
                "loss": loss,
                "acc": acc,
                "some_text": "Some text %d" % it,
                "augmentation image": db.binary("assets/1.jpeg", "%d.jpg" % it)
            }
        )

    db.log_summary({"acc": accuracy})
    
    db.log_artifact({
        "confusion matrix": db.binary("assets/1.jpeg")
    })
    db.log_artifact({"log": db.binary("test.log")})


## Limitations
If `float` is being recorded in database, column with same name will not be converted to another type e.g. `dict`.
