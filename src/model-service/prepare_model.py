from preprocessing.preprocessor import Preprocessor
import lightgbm as lgb
import structlog
import os


logger = structlog.get_logger()

X_train, X_test, y_train, y_test = Preprocessor.run()

param = {'num_leaves': 31, 'objective': 'binary'}
param['metric'] = 'auc'
num_round = 10
booster = lgb.LGBMClassifier(learning_rate=0.25, max_depth=-8, num_leaves=4, random_state=42)
booster.fit(X_train,y_train,eval_set=[(X_test,y_test),(X_train,y_train)], eval_metric='logloss')

logger.info('Training accuracy {:.4f}'.format(booster.score(X_train,y_train)))
logger.info('Testing accuracy {:.4f}'.format(booster.score(X_test,y_test)))
