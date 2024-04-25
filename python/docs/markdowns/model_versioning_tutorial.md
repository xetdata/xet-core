Kickstart ML Project
====================

Let's build a comprehensive machine learning project that scales for almost any smart solution.

## Setup

Let’s start by forking and cloning [this repo](https://xethub.com/xdssio/kickstart_ml) and setting the virtual environment:

```bash
$ git xet clone "https://xethub.com/${XET_USER_NAME}/kickstart_ml.git"
$ cd kickstart_ml
$ python -m venv .venv && . .venv/bin/activate
(.venv) $ pip install -r requirements.txt
```

## Train

Before we start, checkout a baseline branch.
```sh
git checkout -b baseline
```

The train.ipynb Jupyter Notebook in the repository will:

1. Download the [Titanic dataset](https://www.kaggle.com/c/titanic).
2. Build a model.
3. Run evaluation.
4. Save the model, the data, and the metrics to files.

We can run the entire notebook as follows:
```sh
  (cd notebooks && ipython -c "%run train.ipynb")
```
For more sophisticated execution, use [papermill](https://papermill.readthedocs.io/en/latest/index.html).

Let’s push everything into our repository and merge it to main.

```bash
  git push --set-upstream origin baseline
  git add . && git commit -m "baseline training" && git push 
  git checkout main && git merge baseline && git push
```

**Congratulations!** You completed your first project!

<aside>
💡 Note that this would work even if the model or data files are huge.
</aside>



# Next step

## Data Setup

Should we save our data in the same repo? That is absolutely possible, but as your project scales, more data may be ingested
from other services. You might want to have different permissions for adding/removing data and would want to manage it
differently than your machine learning code. Note that while our example uses ML and involves saving our machine learning 
datasets, XetHub can hold any type of file: A/B test data, databases backup dumps, etc.

[Create a new data repository](https://xethub.com/xet/create) and call it `kickstart_data`. Add the data straight to it:

```python
import pyxet

fs = pyxet.XetFS()

# a transaction is needed for write
with fs.transaction as tr:
    tr.set_commit_message("Adding data")
    fs.cp("data/titanic.csv", "xet://${XET_USER_NAME}/kickstart_data/main/titanic.csv")
```

We can delete our local data file: `rm -rf data`.

<aside>
💡 We are no longer afraid to delete data whenever we want because we can revert anything with Git.

</aside>

We use the [git submodule](https://git-scm.com/docs/git-submodule) to clone the *kickstart_data* repository instead.

```bash
git submodule add --force "https://xethub.com/${XET_USER_NAME}/kickstart_data data"
```

- If you don’t see the *titanic.csv* file inside the folder, try
  `(cd data && git pull && git xet checkout --)`
  which will materialise the file from a pointer.
    - This is very important in case you’re using big data.

Let’s adjust our Jupyter Notebook to load the data from “local” and not save it.

```bash
...
# df = pd.read_csv("xet://xdssio/titanic/main/titanic.csv") <-- delete
df = pd.read_csv("../data/titanic.csv")
...
# df.to_csv('../data/data.csv', index=False) <-- delete
```

We can push our changes, and now we manage our data and project with Git 💪!

```bash
(cd notebooks && ipython -c "%run train.ipynb") # retrain (for testing)
git add . && git commit -m "moving data to submodule" && git push 
```

Now other teammates can upload data, and we can pull it.

This is great for reproducing and for re-training cycles, as we show later

## Deployment

Let’s build a [FastAPI](https://fastapi.tiangolo.com/lo/) app to give us predictions.

- This can be done also in a different repo and with a **submodule** but for the sake of simplicity here, we’ll keep it
  all in the same repo.

First we create an app branch and install our new requirements.

```bash
git checkout -b app
pip install fastapi uvicorn pytest
```

To save time, we simply copy it from a ready *app* branch.

```python
import pyxet

fs = pyxet.XetFS()
fs.cp("xdssio/kickstart_ml/app/server", "server")
```

<aside>
💡 How nice is it that we can simply copy code from any repo and any branch?

</aside>

- Test with: `pytest server/tests`
- Deploy with: `unicorn server.app:app --reload`
- Query:

    ```bash
    curl -X 'POST' \
      'http://127.0.0.1:8000/predict' \
      -H 'accept: application/json' \
      -H 'Content-Type: application/json' \
      -d '[
      {
        "Pclass": 0,
        "SibSp": 0,
        "Parch": 0
      }
    ]'
    ```

Let’s have it as part of our project:

```bash
git add server && git commit -m "add fastapi app" && git push
```

As a best practice, let’s have our *production* code in a *************prod*** branch.

```bash
git checkout -b prod && git push
```

## Experiments

Our model is pretty simple - let’s up our game a bit.

`git checkout -b experiment1`

Let’s add [xgboost](https://xgboost.readthedocs.io/en/stable/) and change add some feature engineering:

```bash
num_pipeline = Pipeline([
    ('imputer', SimpleImputer(strategy='mean')),
    ('scaler', StandardScaler())
])
cat_pipeline = Pipeline([
    ('encoder', OneHotEncoder())
])
preprocessor = ColumnTransformer([
    ('num', num_pipeline, ['Age', 'Fare']),
    ('cat', cat_pipeline, ['Sex', 'Embarked'])
])
pipeline = Pipeline([
    ('preprocessor', preprocessor),
    ('classifier', XGBClassifier())
])
```

To get the full code… we’ll copy it from the existing version:

```python
import pyxet

fs = pyxet.XetFS()

fs.cp("xdssio/titanic/experiment1/notebooks/train.ipynb", "notebooks/train.ipynb")
```

if we run our tests (`pytest app/tests`) we’ll see it **failed**! we need to fix our app…

We change the server/app *Query* object and the example in our tests:

```bash
# server/app.py
class Query(BaseModel):
    Age: float
    Fare: float
    Sex: str
    Embarked: str

# server/tests/app_test.py
@pytest.fixture
def example():
    return [{"Sex": "male", "Age": 22.0, "Fare": 7.25, "Embarked": "S"}]
```

- Currently XetHub doesn’t support git-workflows but in the future, this tests can be done automatically before a merge
  as a standard CICD.

```bash
git add . \
  && git commit -m "experiment with Sex, Age, Fare, Embarked and XGBoost" \
  && git push
```

<aside>
💡 Bonus - 
checkout [https://xethub.com/<user-name>/kickstart_ml//experiment1/metrics/results.csv](https://xethub.com/xdssio/kickstart_ml/src/branch/experiment1/metrics/results.csv) to view a visualisation of your results.

</aside>

# Merge new model

`git checkout prod`

First we compare the results to see the model is better - we’ll compare the *accuracy* on *weighted avg.*

- This can be done from any branch.

```bash
import pyxet
import os
import pandas as pd

username = os.getenv('XET_USER_NAME')
results = []
for branch in ["prod", "experiment1"]:
    results.append(pd.read_csv(pyxet.open(f"xet://{username}/kickstart_ml/{branch}/metrics/results.csv")))

df = pd.concat(results)
df = df[df['target']=='weighted avg']
df[['precision','recall','f1-score','accuracy','branch']]
```

```bash
precision    recall  f1-score  accuracy       branch
3   0.729375  0.731844  0.729102  0.731844         prod
3   0.780827  0.782123  0.780847  0.782123  experiment1
```

Looks good!

Let’s merge the new model to prod:

`git merge experiment1 && git push`

Congratulations you are managing you ML project like a boss!

# Retrain with more data

Let’s imagine you get more data from the backend which saved onto our data repo.

We simulate it by just adding data there:

```python
import pyxet

fs = pyxet.XetFS()
with fs.transaction as tr:
    tr.set_commit_message("Adding more data")
    fs.cp("data/titanic.csv", "xet://${XET_USER_NAME}/kickstart_data/main/titanic2.csv")
```

We can have any naming convention for our ”training-cycle-jobs” branches.

```bash
git checkout -b retrain
(cd data && git pull && git xet checkout --) # we'll get us the data localy 
```

We fix our *train.ipynb*

```bash
import glob

# df = pd.read_csv("../data/titanic.csv")  # replace this
df = pd.concat(map(pd.read_csv, glob.glob('../data/*.csv')))
```

Retrain: `(cd notebooks && ipython -c "%run train.ipynb")`

We can compare the results and merge to production like before.

Some options:

- You can revert all your models and deployments.
- If you decide to have every training with a new branch name like: `retrain/v1` for example, you could always compare
  them and create dashboards and alerts
- You can automate this “checkout-get-data-train-merge” cycle with those few lines of code.

For more about XetHub, use-cases and examples, checkout these:
