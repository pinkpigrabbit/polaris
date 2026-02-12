
import asyncio
import sys


# psycopg async on Windows requires SelectorEventLoop.
if sys.platform.startswith("win") and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
