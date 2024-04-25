Writing files with pyxet
========================

## Create your own Titanic repository

Let's walk through a more complete demo of how to use pyxet for some basic ML.

Use the XetHub UI to [create a new repository](https://xethub.com/xet/create). Name the repository `titanic`.

You can then create a branch called experiment-1 with

```
xet branch make xet://<username>/titanic main experiment-1
```

Start a new virtualenv and install some dependencies:

```sh
$ python -m venv .venv
$ . .venv/bin/activate
$ pip install pyxet scikit-learn ipython pandas
```

From your `experiment-1` branch, train and evaluate: 
```python
import pyxet
import json
import pickle
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

df = pd.read_csv("xet://XetHub/titanic/main/titanic.csv")  # read data from XetHub

# Standard ML workflow
target_names, features, target = ['die', 'survive'], ["Pclass", "SibSp", "Parch"], "Survived"

test_size, random_state = 0.2, 42
train, test = train_test_split(df, test_size=test_size, random_state=random_state)
model = RandomForestClassifier().fit(train[features], train[target])
predictions = model.predict(test[features])
print(classification_report(test[target], predictions, target_names=target_names))

# Save important parameters
info = classification_report(test[target], predictions,
                             target_names=target_names,
                             output_dict=True)
info["test_size"] = test_size
info["random_state"] = random_state
info['features'] = features
info['target'] = target

# Record metrics for comparison
results = pd.DataFrame([{'accuracy': info['accuracy'],
                         'precision': info['macro avg']['precision'],
                         'recall': info['macro avg']['recall']}])
```

### Writing back to XetHub

After training your model, you can persist both the model and metrics back to XetHub.
Update the `<user_name>` fields below and run:

```python
fs = pyxet.XetFS()
with fs.transaction as tr:
    tr.set_commit_message("Write experiment 1 results back to repo")
    fs.mkdirs("<user_name>/titanic/experiment-1/metrics", exist_ok=True)
    fs.mkdirs("<user_name>/titanic/experiment-1/models", exist_ok=True)
    results.to_csv(fs.open("<user_name>/titanic/experiment-1/metrics/results.csv", "w"), index=False)  # write results
    pickle.dump(model, fs.open("<user_name>/titanic/experiment-1/models/model.pickle", 'wb'))  # save model
    json.dump(info, fs.open("<user_name>/titanic/experiment-1/metrics/info.json", 'w'))  # any other metadata
```

If you navigate to your titanic repository on XetHub, you'll see the new files show up 
with the corresponding commit in your `experiment-1` branch.

### Loading models

You can easily load a XetHub model from an inference server:

```python
import pyxet
import pickle

model = pickle.load(fs.open("<user_name>/titanic/experiment-1/models/model.pickle", 'rb')) 
```

### Comparing across branches

Versioned experiments on branches enables easy comparison.
To try this out, create a new `experiment-2` branch:
```sh
xet branch make xet://<username>/titanic main experiment-2
```

Run the same code as above, but change the `test_size` and `random_state` values. This time, persist 
your model and metrics back to XetHub in the `experiment-2` branch.

```python
fs = pyxet.XetFS()
with fs.transaction as tr:
    tr.set_commit_message("Write experiment 2 results back to repo")
    fs.mkdirs("<user_name>/titanic/experiment-2/metrics", exist_ok=True)
    fs.mkdirs("<user_name>/titanic/experiment-2/models", exist_ok=True)
    results.to_csv(fs.open("<user_name>/titanic/experiment-2/metrics/results.csv", "w"), index=False)  # write results
    pickle.dump(model, fs.open("<user_name>/titanic/experiment-2/models/model.pickle", 'wb'))  # save model
    json.dump(info, fs.open("<user_name>/titanic/experiment-2/metrics/info.json", 'w'))  # any other metadata
```

Compare your results, making sure to update `<user_name>` in the code below:

```python
import pyxet
import pandas as pd

dfs = []
for branch in ['experiment-1', 'experiment-2']:
    df = pd.read_csv(f"xet://<user_name>/titanic/{branch}/metrics/results.csv")
    df['branch'] = branch
    dfs.append(df)
pd.concat(dfs)
```

Changing `test_size` and `random_state` to 0.5 and 30 respectively results in the following comparison printout:

```sh
   accuracy  precision    recall        branch
0  0.731844   0.724591  0.715573  experiment-1
0  0.688341   0.673824  0.653577  experiment-2
```
