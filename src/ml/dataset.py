from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import torch
from gensim.models import KeyedVectors
from sklearn.preprocessing import StandardScaler
from torch.utils.data import Dataset

RAW_NUMERIC_COLS = [
    "ret_1", "ret_60", "ret_120",
    "ner_org_weight_sum_mean",
    "ner_org_weight_sum_max",
    "ner_n_index_components_sum",
    "ner_has_top_company_any",
]

EMBED_DIM = 300


def cyclic_encode(value: int | float, period: int) -> tuple[float, float]:
    angle = 2.0 * math.pi * (value / period)
    return math.sin(angle), math.cos(angle)


def build_numeric_row(row: pd.Series) -> np.ndarray:
    # 7 raw + sin/cos hour + sin/cos dow = 11 фич
    hour_sin, hour_cos = cyclic_encode(row["hour_of_day"], 24)
    dow_sin, dow_cos = cyclic_encode(row["day_of_week"], 7)
    return np.array(
        [
            row["ret_1"], row["ret_60"], row["ret_120"],
            float(row["ner_org_weight_sum_mean"]),
            float(row["ner_org_weight_sum_max"]),
            float(row["ner_n_index_components_sum"]),
            1.0 if row["ner_has_top_company_any"] else 0.0,
            hour_sin, hour_cos, dow_sin, dow_cos,
        ],
        dtype=np.float32,
    )


NUMERIC_DIM = 11


def embed_news(text: str, kv: KeyedVectors, dim: int = EMBED_DIM) -> np.ndarray:
    vecs = [kv[tok] for tok in text.split() if tok in kv]
    if not vecs:
        return np.zeros(dim, dtype=np.float32)
    return np.mean(vecs, axis=0).astype(np.float32)


@dataclass
class FitState:
    numeric_scaler: StandardScaler


class NewsLSTMDataset(Dataset):
    def __init__(
        self,
        parquet_path: Path,
        kv: KeyedVectors,
        fit_state: FitState | None = None,
    ) -> None:
        df = pd.read_parquet(parquet_path)
        df = df.sort_values("dt").reset_index(drop=True)

        self.text_embs: list[np.ndarray] = []
        for seq in df["text_sequence"]:
            if not isinstance(seq, (list, np.ndarray)) or len(seq) == 0:
                self.text_embs.append(np.zeros((0, EMBED_DIM), dtype=np.float32))
                continue
            news_vecs = np.stack([embed_news(t, kv) for t in seq])
            self.text_embs.append(news_vecs)

        numeric_raw = np.stack([build_numeric_row(r) for _, r in df.iterrows()])

        if fit_state is None:
            scaler = StandardScaler().fit(numeric_raw)
            self.fit_state = FitState(numeric_scaler=scaler)
        else:
            self.fit_state = fit_state

        self.numeric = self.fit_state.numeric_scaler.transform(numeric_raw).astype(np.float32)
        self.targets = df["target_ret_next"].values.astype(np.float32)
        self.timestamps = df["dt"].astype(str).tolist()

    def __len__(self) -> int:
        return len(self.targets)

    def __getitem__(self, idx: int) -> tuple[np.ndarray, np.ndarray, float]:
        return self.text_embs[idx], self.numeric[idx], self.targets[idx]


def collate(batch: Iterable[tuple[np.ndarray, np.ndarray, float]]):
    text_embs, numerics, targets = zip(*batch)
    B = len(batch)
    lengths = torch.tensor([t.shape[0] for t in text_embs], dtype=torch.long)
    max_len = max(1, int(lengths.max().item()))
    padded = torch.zeros(B, max_len, EMBED_DIM, dtype=torch.float32)
    for i, t in enumerate(text_embs):
        if t.shape[0]:
            padded[i, : t.shape[0]] = torch.from_numpy(t)
    numeric_t = torch.from_numpy(np.stack(numerics))
    target_t = torch.tensor(targets, dtype=torch.float32)
    return padded, lengths, numeric_t, target_t
