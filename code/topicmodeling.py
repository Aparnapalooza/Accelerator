import os
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
import pandas as pd
from bertopic import BERTopic
from dotenv import load_dotenv


comments = pd.read_csv("/Users/arama1/Desktop/Accelerator/Accelerator/code/essure_comments_topicmod.csv")
topic_model = BERTopic(language="multilingual", embedding_model="all-MiniLM-L6-v2")
topics, probs = topic_model.fit_transform(comments["Processed_body"].astype(str))

topic_model.get_topic_info()