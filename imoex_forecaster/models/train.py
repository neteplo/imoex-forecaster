import subprocess

import hydra
import pandas as pd
import pytorch_lightning as pl
import torch
from omegaconf import DictConfig
from torch import nn
from torch.utils.data import DataLoader, Dataset

from imoex_forecaster.data.make_dataset import make_dataset


class SimpleDataset(Dataset):
    def __init__(self, features, targets):
        self.features = features
        self.targets = targets

    def __len__(self):
        return len(self.targets)

    def __getitem__(self, idx):
        x = self.features[idx]
        y = self.targets[idx]
        return x, y


class SimpleNN(pl.LightningModule):
    def __init__(self, input_dim, output_dim, learning_rate):
        super(SimpleNN, self).__init__()
        self.layer_1 = nn.Linear(input_dim, 64)
        self.layer_2 = nn.Linear(64, 64)
        self.layer_3 = nn.Linear(64, output_dim)
        self.loss_fn = nn.MSELoss()
        self.learning_rate = learning_rate

    def forward(self, x):
        x = torch.relu(self.layer_1(x))
        x = torch.relu(self.layer_2(x))
        return self.layer_3(x)

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self(x)
        loss = self.loss_fn(y_hat, y)
        self.log("train_loss", loss)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.learning_rate)


def pull_data_with_dvc():
    """
    Выполняет команду `dvc pull` для загрузки данных из удаленного хранилища.
    """
    try:
        subprocess.run(["dvc", "pull"], check=True)
        print("Data pulled successfully from DVC remote.")
    except subprocess.CalledProcessError as e:
        print(f"Error pulling data: {e}")


@hydra.main(version_base=None, config_path="../../conf", config_name="config")
def main(
    config: DictConfig, from_dt: str = "2022-03-07", till_dt: str = "2022-03-07"
):
    # Загрузка сырых данных из удаленного хранилища
    pull_data_with_dvc()

    make_dataset(
        from_dt=from_dt,
        till_dt=till_dt,
        key_id=config["data_loading"]["s3_key_id"],
        secret_key=config["data_loading"]["s3_secret_key"],
        data_dir=config["data_loading"]["local_data_dir"],
    )

    # Загрузка предобработанных данных
    df = pd.read_csv(f"{config['data_loading']['local_data_dir']}/dataset.csv", index_col=["dt"])
    features = torch.tensor(df.drop(columns=["target"]).values, dtype=torch.float32)
    targets = torch.tensor(df["target"].values, dtype=torch.float32)

    dataset = SimpleDataset(features, targets)
    dataloader = DataLoader(
        dataset, batch_size=config["training"]["batch_size"], shuffle=True
    )

    model = SimpleNN(
        input_dim=config["model"]["input_dim"],
        output_dim=config["model"]["output_dim"],
        learning_rate=config["training"]["learning_rate"],
    )
    trainer = pl.Trainer(max_epochs=config["training"]["max_epochs"])
    trainer.fit(model, dataloader)


if __name__ == "__main__":
    main()
