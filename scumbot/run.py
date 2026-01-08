import asyncio
import logging

from .logging_utils import setup_logging
from .bot.app import create_bot, get_token
from .downloader import run_downloader


async def _main_async() -> None:
    """Run discord bot and log downloader concurrently in one asyncio loop."""
    bot = create_bot()
    token = get_token()

    downloader_task = asyncio.create_task(run_downloader(), name="scumbot_downloader")

    try:
        await bot.start(token)
    finally:
        # When the bot stops, ensure downloader stops too.
        if not downloader_task.done():
            downloader_task.cancel()
            try:
                await downloader_task
            except asyncio.CancelledError:
                pass


def main() -> None:
    setup_logging()
    logging.getLogger("boot").info("Starting SCUMBot runtime (bot + downloader) ...")
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        logging.getLogger("boot").info("Exited (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
