__all__ = ["run_order_lab"]


def __getattr__(name: str):
    if name == "run_order_lab":
        from src_dev.orders.lab import run_order_lab

        return run_order_lab
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")