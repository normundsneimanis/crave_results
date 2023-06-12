import random
import time
from crave_results import CraveResults


db = CraveResults()
config = {
    "asd": random.randint(0, 10000)
}
db.init({
    "project": "test_large",
    "notes": "tweak baseline",
    "tags": ["baseline", "paper1"],
    "config": config
})


while True:
    db.log({
        "loss": random.randint(0, 10000)/10000,
        "acc": random.randint(0, 10000)/10000,
        "f1": random.randint(0, 10000)/10000,
    })
    time.sleep(random.randint(0, 1)/100)