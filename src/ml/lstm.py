import torch
from torch import nn
from torch.nn.utils.rnn import pack_padded_sequence


class NewsLSTM(nn.Module):
    def __init__(
        self,
        embed_dim: int = 300,
        hidden_size: int = 256,
        num_layers: int = 1,
        num_numeric: int = 12,
        dropout: float = 0.2,
    ) -> None:
        super().__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(
            input_size=embed_dim,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.head_dropout = nn.Dropout(dropout)
        self.head = nn.Linear(hidden_size + num_numeric, 1)

    def forward(
        self,
        text_emb: torch.Tensor,
        lengths: torch.Tensor,
        numeric: torch.Tensor,
    ) -> torch.Tensor:
        B = text_emb.size(0)
        h_t = torch.zeros(B, self.hidden_size, device=text_emb.device)
        nonempty = lengths > 0
        if nonempty.any():
            packed = pack_padded_sequence(
                text_emb[nonempty], lengths[nonempty].cpu(),
                batch_first=True, enforce_sorted=False,
            )
            _, (h_n, _) = self.lstm(packed)
            h_t[nonempty] = h_n[-1]
        h_t = self.head_dropout(h_t)
        return self.head(torch.cat([h_t, numeric], dim=1)).squeeze(-1)
