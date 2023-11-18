import pandas as pd
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from joblib import dump

PATH_MODEL = './'
FULL_DATASET_PATH = '/app/clean_data/latest_data.csv'
BEST_MODEL_PATH = '/app/clean_data/best_model.pickle'

def compute_model_score(model, X, y):
    # Computing cross-validation scores
    cross_validation = cross_val_score(
        model,
        X,
        y,
        cv=3,
        scoring='neg_mean_squared_error')

    # Convert to positive values for clarity
    model_score = -cross_validation.mean()
    return model_score

def train_and_save_model(model, X, y, path_to_model='./app/model.pickle'):
    model.fit(X, y)
    print(f'{str(model)} saved at {path_to_model}')
    dump(model, path_to_model)

def prepare_data(path_to_data=FULL_DATASET_PATH):
    df = pd.read_csv(path_to_data)
    df.sort_values(['city', 'date'], ascending=True, inplace=True)

    dfs = []
    for c in df['city'].unique():
        df_temp = df[df['city'] == c].copy()

        # Creating target and features
        df_temp['target'] = df_temp['temperature'].shift(1)
        for i in range(1, 10):
            df_temp[f'temp_m-{i}'] = df_temp['temperature'].shift(-i)

        # Deleting rows with null values
        df_temp.dropna(inplace=True)
        dfs.append(df_temp)

    # Concatenating datasets
    df_final = pd.concat(dfs, axis=0, ignore_index=True)
    df_final.drop(['date'], axis=1, inplace=True)

    # Creating dummies
    df_final = pd.get_dummies(df_final)
    features = df_final.drop(['target'], axis=1)
    target = df_final['target']

    return features, target

def compute_score_and_save(model_name, task_instance):
    X, y = prepare_data()
    model_mapping = {
        "LinearRegression": LinearRegression(),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(),
    }

    score = compute_model_score(model_mapping[model_name], X, y)
    task_instance.xcom_push(key=model_name, value=score)

def choose_model_and_train(task_instance):
    X, y = prepare_data()
    model_mapping = {
        "LinearRegression": LinearRegression(),
        "DecisionTreeRegressor": DecisionTreeRegressor(),
        "RandomForestRegressor": RandomForestRegressor(),
    }

    # Retrieving scores from previous tasks
    model_performance = {
        "LinearRegression": task_instance.xcom_pull(key="LinearRegression", task_ids=['train_linear_regression_model'])[0],
        "DecisionTreeRegressor": task_instance.xcom_pull(key="DecisionTreeRegressor", task_ids=['train_decision_tree_model'])[0],
        "RandomForestRegressor": task_instance.xcom_pull(key="RandomForestRegressor", task_ids=['train_random_forest_model'])[0],
    }

    # Filtering out None values
    model_performance = {k: v for k, v in model_performance.items() if v is not None}

    # Validating model performance data
    if not model_performance:
        raise ValueError("All models resulted in None performance scores.")

    # Selecting the best model
    best_model = max(model_performance, key=model_performance.get)

    # Training and saving the best model
    train_and_save_model(model_mapping[best_model], X, y, BEST_MODEL_PATH)


