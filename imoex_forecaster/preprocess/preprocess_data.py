import re

import nltk
import pandas as pd
import pymorphy2
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import TfidfVectorizer

try:
    _ = stopwords.words("russian")
except LookupError:
    nltk.download("stopwords")

russian_stopwords = set(stopwords.words("russian"))
morph = pymorphy2.MorphAnalyzer()


def clear_text(text: str) -> str:
    """
    Выполняет предобработку заданной строки текста на русском языке, включая:
    - Преобразование текста в нижний регистр.
    - Удаление знаков препинания и цифр.
    - Токенизация текста и удаление стоп-слов на русском языке.
    - Лемматизация токенов до их базовых форм.

    Args:
        text (str): Входная строка текста на русском языке для предобработки.

    Returns:
        str: Одна строка, содержащая предобработанный текст, с токенами,
             соединенными пробелами.
    """

    text = text.lower()

    text = re.sub(r"[^\w\s]", "", text)
    text = re.sub(r"\d+", "", text)

    tokens = text.split()
    tokens = [token for token in tokens if token not in russian_stopwords]

    lemmatized_tokens = [morph.parse(token)[0].normal_form for token in tokens]
    return " ".join(lemmatized_tokens)


def vectorize_data(text_arr) -> pd.DataFrame:
    tfidf_vectorizer = TfidfVectorizer(max_features=1000)

    vectorized_text = tfidf_vectorizer.fit_transform(text_arr)

    vectorized_df = pd.DataFrame(
        vectorized_text.toarray(), columns=tfidf_vectorizer.get_feature_names_out()
    )

    return vectorized_df


def preprocess(data) -> pd.DataFrame:
    pass
