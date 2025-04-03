import argparse
import pickle
import time
from pathlib import Path

import numpy as np
import torch
from gensim.models import KeyedVectors
from torch.utils.data import DataLoader

from src.ml.dataset import (
    EMBED_DIM,
    NUMERIC_DIM,
    NewsLSTMDataset,
    collate,
)
from src.ml.eval import evaluate, format_metrics
from src.ml.lstm import NewsLSTM

DEFAULT_TRAIN = Path("data/processed/train.parquet")
DEFAULT_VAL = Path("data/processed/val.parquet")
DEFAULT_TEST = Path("data/processed/test.parquet")
DEFAULT_W2V = Path("models/word2vec.kv")
DEFAULT_OUT_DIR = Path("models")


def predict(model: NewsLSTM, loader: DataLoader, device: torch.device) -> tuple[np.ndarray, np.ndarray]:
    model.eval()
    preds, trues = [], []
    with torch.no_grad():
        for text_emb, lengths, numeric, target in loader:
            text_emb = text_emb.to(device)
            numeric = numeric.to(device)
            y_pred = model(text_emb, lengths, numeric).cpu().numpy()
            preds.append(y_pred)
            trues.append(target.numpy())
    return np.concatenate(preds), np.concatenate(trues)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Обучение NewsLSTM на parquet-сплитах.")
    p.add_argument("--train", type=Path, default=DEFAULT_TRAIN)
    p.add_argument("--val", type=Path, default=DEFAULT_VAL)
    p.add_argument("--test", type=Path, default=DEFAULT_TEST)
    p.add_argument("--w2v", type=Path, default=DEFAULT_W2V)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--epochs", type=int, default=50)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--lr", type=float, default=1e-3)
    p.add_argument("--hidden-size", type=int, default=256)
    p.add_argument("--dropout", type=float, default=0.2)
    p.add_argument("--patience", type=int, default=5)
    p.add_argument("--device", default=None, help="cpu / cuda / mps, иначе auto")
    p.add_argument("--no-export", action="store_true")
    return p.parse_args()


def pick_device(arg: str | None) -> torch.device:
    if arg:
        if arg == "cuda" and not torch.cuda.is_available():
            raise SystemExit(
                "--device cuda, но torch.cuda.is_available()=False. "
                "Либо runtime собран без CUDA (CPU-only torch), либо в Colab "
                "не выбран GPU: Runtime → Change runtime type → T4 GPU → Restart."
            )
        return torch.device(arg)
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def main() -> None:
    args = parse_args()
    device = pick_device(args.device)
    print(f"Device: {device}\n")

    print(f"Загружаю W2V: {args.w2v}")
    kv = KeyedVectors.load(str(args.w2v))
    assert kv.vector_size == EMBED_DIM, f"W2V dim={kv.vector_size}, expected {EMBED_DIM}"

    print(f"Готовлю train: {args.train}")
    train_ds = NewsLSTMDataset(args.train, kv)
    print(f"Готовлю val:   {args.val}")
    val_ds = NewsLSTMDataset(args.val, kv, fit_state=train_ds.fit_state)
    print(f"Готовлю test:  {args.test}")
    test_ds = NewsLSTMDataset(args.test, kv, fit_state=train_ds.fit_state)
    print(f"Сплиты: train={len(train_ds)}, val={len(val_ds)}, test={len(test_ds)}\n")

    train_loader = DataLoader(
        train_ds, batch_size=args.batch_size, shuffle=True,
        collate_fn=collate, drop_last=False,
    )
    val_loader = DataLoader(
        val_ds, batch_size=args.batch_size, shuffle=False, collate_fn=collate,
    )
    test_loader = DataLoader(
        test_ds, batch_size=args.batch_size, shuffle=False, collate_fn=collate,
    )

    model = NewsLSTM(
        embed_dim=EMBED_DIM,
        hidden_size=args.hidden_size,
        num_layers=1,
        num_numeric=NUMERIC_DIM,
        dropout=args.dropout,
    ).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
    loss_fn = torch.nn.MSELoss()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    best_val_mse = float("inf")
    best_path = args.out_dir / "lstm_best.pt"
    scaler_path = args.out_dir / "lstm_scaler.pkl"
    patience_left = args.patience

    print("Старт обучения\n")
    for epoch in range(1, args.epochs + 1):
        t0 = time.time()
        model.train()
        train_losses = []
        for text_emb, lengths, numeric, target in train_loader:
            text_emb = text_emb.to(device)
            numeric = numeric.to(device)
            target = target.to(device)
            optimizer.zero_grad()
            pred = model(text_emb, lengths, numeric)
            loss = loss_fn(pred, target)
            loss.backward()
            optimizer.step()
            train_losses.append(loss.item())

        train_mse = float(np.mean(train_losses))
        val_pred, val_true = predict(model, val_loader, device)
        val_m = evaluate(val_true, val_pred)
        dt_s = time.time() - t0
        print(
            f"epoch {epoch:3d}  train_mse={train_mse:.3e}  "
            f"val_mse={val_m['mse']:.3e}  val_dir={val_m['dir_acc']:.1%}  "
            f"vs_naive={val_m['mse'] / val_m['mse_naive']:.3f}  ({dt_s:.1f}s)"
        )

        if val_m["mse"] < best_val_mse - 1e-12:
            best_val_mse = val_m["mse"]
            patience_left = args.patience
            torch.save(model.state_dict(), best_path)
            with scaler_path.open("wb") as f:
                pickle.dump(train_ds.fit_state, f)
        else:
            patience_left -= 1
            if patience_left <= 0:
                print(f"\nEarly stopping на эпохе {epoch} (patience exhausted)")
                break

    print(f"\nBest val MSE: {best_val_mse:.3e}  ({best_path})")

    model.load_state_dict(torch.load(best_path, map_location=device))
    test_pred, test_true = predict(model, test_loader, device)
    test_m = evaluate(test_true, test_pred)
    train_pred, train_true = predict(model, train_loader, device)
    train_m = evaluate(train_true, train_pred)
    val_pred, val_true = predict(model, val_loader, device)
    val_m = evaluate(val_true, val_pred)
    print("\n=== Финальные метрики ===")
    print(format_metrics("train", train_m))
    print(format_metrics("val", val_m))
    print(format_metrics("test", test_m))

    if not args.no_export:
        export_path = args.out_dir / "lstm_model.pt"
        try:
            scripted = torch.jit.script(model.cpu())
            scripted.save(str(export_path))
            print(f"\nTorchScript-экспорт: {export_path}")
        except Exception as e:
            print(f"\nTorchScript-экспорт упал: {e!r} — пропускаю.")


if __name__ == "__main__":
    main()
