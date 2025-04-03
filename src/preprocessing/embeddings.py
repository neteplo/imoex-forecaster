import argparse
from pathlib import Path

import pandas as pd
from gensim.models import Word2Vec

DEFAULT_CORPUS = Path("data/processed/news_clean.parquet")
DEFAULT_OUT_MODEL = Path("models/word2vec.model")
DEFAULT_OUT_KV = Path("models/word2vec.kv")


def tokenize(text: str) -> list[str]:
    # text уже cleaned + lower без пунктуации → split() достаточно
    return text.split()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Обучение Word2Vec на очищенном корпусе.")
    p.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    p.add_argument("--out-model", type=Path, default=DEFAULT_OUT_MODEL)
    p.add_argument("--out-kv", type=Path, default=DEFAULT_OUT_KV)
    p.add_argument("--vector-size", type=int, default=300)
    p.add_argument("--window", type=int, default=5)
    p.add_argument("--min-count", type=int, default=3)
    p.add_argument("--epochs", type=int, default=10)
    p.add_argument("--sg", type=int, choices=[0, 1], default=1, help="0=CBOW, 1=skip-gram")
    p.add_argument("--workers", type=int, default=4)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    print(f"Читаю {args.corpus}…")
    df = pd.read_parquet(args.corpus, columns=["text"])
    print(f"Документов: {len(df)}")

    print("Токенизация…")
    sentences = df["text"].map(tokenize).tolist()
    n_tokens = sum(len(s) for s in sentences)
    print(f"Всего токенов: {n_tokens:,}; средняя длина документа: {n_tokens / len(sentences):.1f}")

    print(
        f"Обучение Word2Vec: dim={args.vector_size}, window={args.window}, "
        f"min_count={args.min_count}, epochs={args.epochs}, sg={args.sg}…"
    )
    model = Word2Vec(
        sentences=sentences,
        vector_size=args.vector_size,
        window=args.window,
        min_count=args.min_count,
        workers=args.workers,
        sg=args.sg,
        epochs=args.epochs,
    )

    args.out_model.parent.mkdir(parents=True, exist_ok=True)
    args.out_kv.parent.mkdir(parents=True, exist_ok=True)
    model.save(str(args.out_model))
    model.wv.save(str(args.out_kv))
    print(f"\nСохранено:")
    print(f"  модель (для дообучения): {args.out_model}")
    print(f"  KeyedVectors (инференс): {args.out_kv}")
    print(f"Размер словаря: {len(model.wv):,}")

    print("\n=== Sanity-check: ближайшие соседи ===")
    for probe in ["сбербанк", "газпром", "рубль", "нефть", "ставка", "санкции"]:
        if probe in model.wv:
            neighbours = model.wv.most_similar(probe, topn=8)
            pretty = ", ".join(f"{w}:{s:.2f}" for w, s in neighbours)
            print(f"  {probe:12s} → {pretty}")
        else:
            print(f"  {probe:12s} — нет в словаре (порог min_count={args.min_count})")


if __name__ == "__main__":
    main()
