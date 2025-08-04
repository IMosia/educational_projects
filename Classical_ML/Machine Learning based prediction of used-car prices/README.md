# edu_cars
Machine Learning based prediction of used-car prices  
**Determining the value of cars**  
**Aim:** to predict the price of used cars based on their parameters with machine-learning algorithms, considering learning and prediction time.  
**Description:** The dataset containing descriptions of used cars was cleaned, namely, outliners were removed and missing values were filled. Categorical parameters were encoded. Based on prepared data a series of models were trained (linear regression, decision tree, random forest, random forest-based boosting models) with hyperparameter optimization and several parameter sets (only numerical data, various categorical data encoding techniques).  
Applied tools: data pre-processing, exploratory data analysis, linear regression, decision tree, random forest, boosting-based models, hyperparameters optimization, parameter encoding.  
**Libraries:** pandas, numpy, matplotlib, seaborn, sklearn, lightgbm, catboost, time.  
**Results:** The impact of parameters encoded on predictions precision and training time was revealed, and a good balance between quality and speed was found for CatBoost model.
