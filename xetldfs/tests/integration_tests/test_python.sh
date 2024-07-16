#!/usr/bin/env bash
set -e
set -x

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" &>/dev/null && pwd 2>/dev/null)"
. "$SCRIPT_DIR/initialize.sh"

. "$SCRIPT_DIR/setup_test_pyenv.sh"

git xet clone --lazy $remote repo

pushd repo

for n in 1 2; do
    assert_is_pointer_file f$n.csv
    py_verify_size f$n.csv $csv_data_file_size
done

for n in 1 2; do
    assert_is_pointer_file f$n.mat
    py_verify_size f$n.mat $mat_data_file_size
done

# pandas Read
(
    xetfs_on

    h=$(py_run "
import pandas as pd
h=pd.read_csv('f1.csv').keys().tolist()
print(','.join(h))
    ")
    [[ "$h" == "$header" ]] || die "pandas read f1.csv header not read as pointer."

    r2_location=$(py_run "
import pandas as pd
df=pd.read_csv('f1.csv')
print(df.at[2, 'location'])
    ")
    [[ "$r2_location" == "$(echo $r2 | cut -d',' -f4)" ]] || die "pandas read f1.csv cell not read as pointer."
)

# pandas Write
(
    xetfs_on

    py_run "
import pandas as pd
df=pd.read_csv('f2.csv')
df.at[2, 'location'] = $r2_new_location
df.to_csv('f2.csv', index=False)
    "
    r2=$(cat f2.csv | head -4 | tail -1)
    [[ "$r2" == "$r2_new" ]] || die "pandas write f2.csv replacing cell value failed"
)

# numpy Read
(
    xetfs_on
    shape=$(py_run "
import numpy as np
arr=np.genfromtxt('f1.mat')
print(arr.shape[0], arr.shape[1])
    ")
    [[ "$shape" == $mat_shape ]] || die "numpy read f1.mat shape not read as pointer."

    r2=$(py_run "
import numpy as np
arr=np.genfromtxt('f1.mat')
np.set_printoptions(precision=2, floatmode='fixed')
print(arr[2])
    ")
    [[ "$r2" == *"$m2"* ]] || die "numpy read f1.mat row 2 not read as pointer."
)

# numpy Write
(
    xetfs_on

    py_run "
import numpy as np
arr=np.genfromtxt('f2.mat')
arr2=arr.reshape(1, arr.size)
np.savetxt('f2.mat', arr2, fmt='%.2f')
    "

    [[ "$(cat f2.mat)" == *"$flat_mat"* ]] || die "numpy write f2.mat reshaping matrix failed"
)

popd