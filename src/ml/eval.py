import numpy as np


def evaluate(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float]:
    y_true = np.asarray(y_true, dtype=np.float64)
    y_pred = np.asarray(y_pred, dtype=np.float64)
    mse = float(np.mean((y_true - y_pred) ** 2))
    mae = float(np.mean(np.abs(y_true - y_pred)))
    dir_acc = float(np.mean(np.sign(y_true) == np.sign(y_pred)))
    # «всегда 0»: var(y) и mean(|y|) — нижняя граница, которую модель должна побить
    mse_naive = float(np.mean(y_true ** 2))
    mae_naive = float(np.mean(np.abs(y_true)))
    return {
        "mse": mse,
        "mae": mae,
        "dir_acc": dir_acc,
        "mse_naive": mse_naive,
        "mae_naive": mae_naive,
        "mse_ratio_vs_naive": mse / mse_naive if mse_naive > 0 else float("nan"),
    }


def format_metrics(name: str, m: dict[str, float]) -> str:
    return (
        f"{name:>6s}  mse={m['mse']:.3e}  mae={m['mae']:.3e}  "
        f"dir_acc={m['dir_acc']:.1%}  vs naive: "
        f"mse={m['mse'] / m['mse_naive']:.3f}, mae={m['mae'] / m['mae_naive']:.3f}"
    )
