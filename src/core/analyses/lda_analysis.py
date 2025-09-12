# TODO
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.datasets import load_iris  # Example dataset
import numpy as np

# Example data
X, y = load_iris(return_X_y=True)

# The maximum number of LDA components is min(n_classes - 1, n_features)
max_k = min(len(np.unique(y)) - 1, X.shape[1])

# Loop over desired k values (1 to max_k)
for k in range(1, max_k + 1):
    lda = LinearDiscriminantAnalysis(n_components=k)
    X_lda = lda.fit_transform(X, y)
    print(f"LDA with {k} component(s): Transformed shape = {X_lda.shape}")
    # You can now use X_lda for further analysis or classification