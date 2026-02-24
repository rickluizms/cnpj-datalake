from rich.progress import Progress, SpinnerColumn, BarColumn, DownloadColumn, TransferSpeedColumn, TimeRemainingColumn


def create_download_progress() -> Progress:
    """Cria um Progress configurado para downloads."""
    return Progress(
        SpinnerColumn(),
        "[bold blue]{task.description}",
        BarColumn(bar_width=30),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    )
