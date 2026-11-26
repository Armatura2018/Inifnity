        message.from_user.username,
        message.from_user.first_name,
    )
    await message.answer(START_TEXT)


async def cmd_help(message: Message) -> None:
    if message.chat.type == "private":
        await message.answer(HELP_TEXT)


async def cmd_status(message: Message) -> None:
    if not message.from_user or message.chat.type != "private":
        return

    rows = await db_fetchall(
        """
        SELECT connection_id, is_enabled, connected_at, rights_json
        FROM business_connections
        WHERE owner_id=?
        ORDER BY updated_at DESC
        """,
        (message.from_user.id,),
    )

    if not rows:
        await message.answer(
            "Business-подключение не найдено.\n\n"
            "Сначала подключи Infinity в настройках Telegram Business."
        )
        return

    lines = ["<b>Статус Infinity</b>\n"]
    for index, row in enumerate(rows, 1):
        state = "🟢 Активно" if row["is_enabled"] else "🔴 Отключено"
        try:
            rights = json.loads(row["rights_json"] or "{}")
        except json.JSONDecodeError:
            rights = {}
        read_access = rights.get("can_read_messages")
        access_text = (
            "доступ к сообщениям подтверждён"
            if read_access
            else "проверь разрешение на чтение сообщений"
        )
        lines.append(
            f"{index}. {state}\n"
            f"   ID: <code>{safe_html(row['connection_id'])}</code>\n"
            f"   {access_text}\n"
            f"   Подключено: {fmt_time(row['connected_at'])}"
        )
    await message.answer("\n\n".join(lines))


async def on_business_connection(connection: BusinessConnection) -> None:
    try:
        await store_connection(connection)
        log.info(
            "Business connection state updated: enabled=%s",
            connection.is_enabled,
        )
    except Exception as exc:
        log.exception("Could not store Business connection (%s)", type(exc).__name__)


async def on_business_message(bot: Bot, message: Message) -> None:
    try:
        await save_business_message(bot, message)
    except Exception as exc:
        log.exception("Could not archive Business message (%s)", type(exc).__name__)


async def on_edited_business_message(bot: Bot, message: Message) -> None:
    try:
        await handle_edited_message(bot, message)
    except Exception as exc:
        log.exception("Could not process edited Business message (%s)", type(exc).__name__)


async def on_deleted_business_messages(
    bot: Bot, deleted: BusinessMessagesDeleted
) -> None:
    try:
        await handle_deleted_messages(bot, deleted)
    except Exception as exc:
        log.exception("Could not process deleted Business messages (%s)", type(exc).__name__)


async def main() -> None:
    await init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    dp.message.register(cmd_start, Command("start"))
    dp.message.register(cmd_status, Command("status"))
    dp.message.register(cmd_help, Command("help"))

    dp.business_connection.register(on_business_connection)
    dp.business_message.register(on_business_message)
    dp.edited_business_message.register(on_edited_business_message)
    dp.deleted_business_messages.register(on_deleted_business_messages)

    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Запустить Infinity"),
            BotCommand(command="status", description="Статус Business-подключения"),
            BotCommand(command="help", description="Инструкция"),
        ]
    )

    cleanup_task = asyncio.create_task(cleanup_loop())
    try:
        # Preserve queued updates after restarts; aiogram polling reconnects with backoff.
        await bot.delete_webhook(drop_pending_updates=False)
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            close_bot_session=True,
        )
    finally:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        if db is not None:
            await db.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Infinity stopped")
