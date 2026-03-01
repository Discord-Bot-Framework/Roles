from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import pathlib
from typing import TYPE_CHECKING, Any, TypedDict

import arc
import hikari
import lmdb
import miru
from miru.ext import nav
from src.container.app import get_miru
from src.shared.constants import GUILD_ID
from src.shared.logger import get_module_logger
from src.shared.persistence.repository import (
    delete_record,
    get_mapping_record,
    list_mapping_records,
    put_mapping_record,
)
from src.shared.persistence.store import Store
from src.shared.utils.view import (
    Color,
    defer,
    reply_embed,
    reply_err,
    reply_ok,
    respond_with_builder_and_bind_view,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import AsyncGenerator, Iterable, Sequence

ELECT_VETTING_FORUM_ID: int = 1164834982737489930
APPR_VETTING_FORUM_ID: int = 1307001955230552075
ELECTORAL_ROLE_ID: int = 1200043628899356702
APPROVED_ROLE_ID: int = 1282944839679344721
TEMPORARY_ROLE_ID: int = 1164761892015833129
REQUIRED_APPROVALS: int = 3
REQUIRED_REJECTIONS: int = 3
REJECTION_WINDOW_DAYS: int = 7
DIVIDER_CONTAINS: str = "[]"
BASE_DIR: pathlib.Path = pathlib.Path(__file__).parent
DB_PATH: pathlib.Path = BASE_DIR / "roles"
DB_MAP_SIZE: int = 50 * 1024 * 1024
DB_STICKY_ROLE: str = "sticky_role"
DBS: tuple[str, ...] = (DB_STICKY_ROLE,)
PROTECTED_PERMISSIONS: hikari.Permissions = (
    hikari.Permissions.ADMINISTRATOR
    | hikari.Permissions.KICK_MEMBERS
    | hikari.Permissions.BAN_MEMBERS
    | hikari.Permissions.MANAGE_GUILD
    | hikari.Permissions.MANAGE_ROLES
    | hikari.Permissions.MANAGE_CHANNELS
    | hikari.Permissions.MANAGE_MESSAGES
    | hikari.Permissions.MANAGE_WEBHOOKS
    | hikari.Permissions.MANAGE_GUILD_EXPRESSIONS
    | hikari.Permissions.MANAGE_THREADS
)
PROTECTED_ROLE_IDS: frozenset[int] = frozenset(
    {
        ELECTORAL_ROLE_ID,
        APPROVED_ROLE_ID,
        TEMPORARY_ROLE_ID,
    },
)

logger: logging.Logger = get_module_logger(__file__, __name__, "roles.log")

plugin = arc.GatewayPlugin("Roles")
group = plugin.include_slash_group("roles", "Role management commands")
group_servant = group.include_subgroup("servant", "Servants management")


class _LockData(TypedDict):
    lock: asyncio.Lock
    last_used: datetime.datetime


@dataclasses.dataclass(slots=True, frozen=True)
class Approval:
    approval_count: int = 0
    rejection_count: int = 0
    reviewers: frozenset[int] = frozenset()
    last_approval_time: datetime.datetime | None = None


class StickyRoleRecord(TypedDict):
    role_ids: list[int]
    updated_at: str


class StickyPayload(TypedDict):
    members: dict[str, StickyRoleRecord]


database = Store(DB_PATH, DBS, map_size=DB_MAP_SIZE)


class BaseDB:
    __slots__ = ()

    @classmethod
    def get_database(cls) -> str:
        raise NotImplementedError

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, Any]]:
        return list_mapping_records(
            env,
            database,
            cls.get_database(),
            strict_map_key=False,
        )

    @classmethod
    def delete(cls, env: lmdb.Environment | None, key: bytes) -> bool:
        return delete_record(env, database, cls.get_database(), key)


class StickyRoleDB(BaseDB):
    __slots__ = ()

    @classmethod
    def get_database(cls) -> str:
        return DB_STICKY_ROLE

    @staticmethod
    def _normalize_member_record(payload: Any) -> StickyRoleRecord | None:
        if not isinstance(payload, dict):
            return None
        raw_roles = payload.get("role_ids")
        if not isinstance(raw_roles, list):
            return None
        unique_roles: list[int] = []
        seen: set[int] = set()
        for role in raw_roles:
            if isinstance(role, int):
                role_id = role
            elif isinstance(role, str) and role.isdigit():
                role_id = int(role)
            else:
                continue
            if role_id <= 0 or role_id in seen:
                continue
            seen.add(role_id)
            unique_roles.append(role_id)
        updated_at = payload.get("updated_at")
        if not isinstance(updated_at, str):
            updated_at = _utcnow().isoformat()
        return {"role_ids": unique_roles, "updated_at": updated_at}

    @classmethod
    def get_member(
        cls,
        env: lmdb.Environment | None,
        member_id: int,
    ) -> StickyRoleRecord | None:
        key = str(member_id).encode("utf-8")
        payload = get_mapping_record(
            env,
            database,
            cls.get_database(),
            key,
            strict_map_key=False,
        )
        if payload is None:
            return None
        return cls._normalize_member_record(payload)

    @classmethod
    def get_databases(cls, env: lmdb.Environment | None) -> list[dict[str, Any]]:
        return BaseDB.get_databases(env)

    @classmethod
    def upsert_member(
        cls,
        env: lmdb.Environment | None,
        member_id: int,
        role_ids: list[int],
        updated_at: str,
    ) -> None:
        payload = {
            "member_id": member_id,
            "role_ids": role_ids,
            "updated_at": updated_at,
        }
        put_mapping_record(
            env,
            database,
            cls.get_database(),
            str(member_id).encode("utf-8"),
            payload,
        )

    @classmethod
    def delete_member(cls, env: lmdb.Environment | None, member_id: int) -> bool:
        return BaseDB.delete(env, str(member_id).encode("utf-8"))


database.open()
env: lmdb.Environment | None = database.env


class RolesState:
    def __init__(
        self,
        client: arc.GatewayClient,
    ) -> None:
        self.client = client
        self.approval_counts: dict[int, Approval] = {}
        self.processed_thread_ids: set[int] = set()
        self.member_role_locks: dict[int, _LockData] = {}

    @property
    def app(self) -> hikari.GatewayBot:
        app = self.client.app
        if not isinstance(app, hikari.GatewayBot):
            msg = "Expected GatewayBot client"
            raise RuntimeError(msg)
        return app

    @property
    def miru_client(self) -> miru.Client:
        return get_miru()


_state: RolesState | None = None


def get_state() -> RolesState:
    state = _state
    if state is None:
        msg = "Failed to initialize Roles state"
        raise RuntimeError(msg)
    return state


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def extract_member_roles(member: hikari.Member) -> list[int]:
    return [int(role_id) for role_id in member.role_ids]


def _is_divider_role(role: hikari.Role) -> bool:
    name = role.name
    return all(char in name for char in DIVIDER_CONTAINS)


def _filter_sticky_restorable_roles(
    guild: hikari.Guild,
    role_ids: Iterable[int],
) -> list[hikari.Role]:
    roles: list[hikari.Role] = []
    for role_id in role_ids:
        role = guild.get_role(int(role_id))
        if role is None:
            continue
        is_protected = (
            bool(role.permissions & PROTECTED_PERMISSIONS)
            or role.id in PROTECTED_ROLE_IDS
        )
        if role.is_managed or is_protected:
            continue
        roles.append(role)
    return roles


def _resolve_workflow_roles(
    guild: hikari.Guild,
    *,
    is_appr_forum: bool,
) -> dict[str, hikari.Role | None]:
    return {
        "electoral": guild.get_role(ELECTORAL_ROLE_ID),
        "approved": guild.get_role(APPROVED_ROLE_ID),
        "temporary": guild.get_role(TEMPORARY_ROLE_ID),
        "required": guild.get_role(
            TEMPORARY_ROLE_ID if is_appr_forum else APPROVED_ROLE_ID,
        ),
        "target": guild.get_role(
            APPROVED_ROLE_ID if is_appr_forum else ELECTORAL_ROLE_ID,
        ),
        "fallback": guild.get_role(
            TEMPORARY_ROLE_ID if is_appr_forum else APPROVED_ROLE_ID,
        ),
    }


def _approval_title(guild: hikari.Guild | None, is_appr_forum: bool) -> str:
    if guild is None:
        return "Role Application"
    role_id = APPROVED_ROLE_ID if is_appr_forum else ELECTORAL_ROLE_ID
    role = guild.get_role(role_id)
    if role is None:
        return "Role Application"
    return f"{role.mention} Role Application"


def _format_reviewers(reviewers: frozenset[int] | set[int]) -> str:
    if not reviewers:
        return "No reviewers"
    return ",".join(f"<@{reviewer_id}>" for reviewer_id in sorted(reviewers))


async def _build_vote_embed(
    state: RolesState,
    *,
    guild: hikari.Guild | None,
    approval: Approval,
    is_appr_forum: bool,
) -> hikari.Embed:
    required_approvals = 1 if is_appr_forum else REQUIRED_APPROVALS
    embed = hikari.Embed(
        title=_approval_title(guild, is_appr_forum),
        description=(
            f"Approving: {approval.approval_count}/{required_approvals}"
            f"\nReviewing: {_format_reviewers(approval.reviewers)}"
        ),
        color=Color.INFO.value,
        timestamp=_utcnow(),
    )

    me = state.app.get_me()
    if me is not None:
        embed.set_author(name=me.username, icon=me.display_avatar_url)

    if guild is not None and guild.icon_hash:
        embed.set_footer(text=guild.name, icon=guild.make_icon_url())

    return embed


async def delete_data(state: RolesState, key: str) -> bool:
    del state
    member_id = int(key) if key.isdigit() else -1
    if member_id <= 0:
        return False
    try:
        deleted = StickyRoleDB.delete_member(env, member_id)
    except lmdb.Error:
        logger.exception("Failed to delete %s from LMDB", key)
        raise
    if deleted:
        logger.info("Deleted %s", key)
    else:
        logger.info("Failed to find %s for deletion", key)
    return deleted


async def read_data(state: RolesState) -> StickyPayload:
    del state
    members: dict[str, StickyRoleRecord] = {}
    for record in StickyRoleDB.get_databases(env):
        member_id = record.get("member_id")
        if not isinstance(member_id, int) or member_id <= 0:
            continue
        normalized = StickyRoleDB._normalize_member_record(record)
        if normalized is None:
            continue
        members[str(member_id)] = normalized
    return {"members": members}


async def write_data(state: RolesState, data: StickyPayload) -> None:
    del state
    try:
        members_in = data.get("members", {})
        if not isinstance(members_in, dict):
            return
        desired_ids: set[int] = set()
        for raw_member_id, payload in members_in.items():
            member_id = int(raw_member_id) if str(raw_member_id).isdigit() else -1
            if member_id <= 0:
                continue
            normalized = StickyRoleDB._normalize_member_record(payload)
            if normalized is None:
                continue
            desired_ids.add(member_id)
            StickyRoleDB.upsert_member(
                env,
                member_id,
                normalized["role_ids"],
                normalized["updated_at"],
            )

        for record in StickyRoleDB.get_databases(env):
            member_id = record.get("member_id")
            if not isinstance(member_id, int) or member_id <= 0:
                continue
            if member_id not in desired_ids:
                StickyRoleDB.delete_member(env, member_id)
    except (TypeError, ValueError, lmdb.Error):
        logger.exception("Failed to write sticky roles payload")
        raise


async def update_sticky_roles(
    state: RolesState,
    member_id: int,
    role_ids: Sequence[int],
) -> None:
    unique_role_ids = list(
        dict.fromkeys(int(role_id) for role_id in role_ids if int(role_id) > 0),
    )
    del state
    StickyRoleDB.upsert_member(env, member_id, unique_role_ids, _utcnow().isoformat())


async def divide_member_roles(state: RolesState, member: hikari.Member) -> None:
    guild = await state.app.rest.fetch_guild(GUILD_ID)
    if guild is None:
        return

    member_role_ids = frozenset(int(role_id) for role_id in member.role_ids)
    needs_divider_after = False

    for role in sorted(guild.get_roles().values(), key=lambda item: item.position):
        if _is_divider_role(role):
            has_role = int(role.id) in member_role_ids
            if needs_divider_after and not has_role:
                await member.add_role(role)
            elif not needs_divider_after and has_role:
                await member.remove_role(role)
            needs_divider_after = False
            continue

        if role.id != guild.id and int(role.id) in member_role_ids:
            needs_divider_after = True


async def initial_data(state: RolesState) -> None:
    now_iso = _utcnow().isoformat()
    members_to_store = 0
    async for member in state.app.rest.fetch_members(GUILD_ID):
        user = member.user
        if user is None:
            continue

        role_ids = extract_member_roles(member)
        if not role_ids:
            continue

        unique_role_ids = list(
            dict.fromkeys(int(role_id) for role_id in role_ids if int(role_id) > 0),
        )
        if not unique_role_ids:
            continue
        StickyRoleDB.upsert_member(env, int(user.id), unique_role_ids, now_iso)
        members_to_store += 1

    logger.info(
        "Initialized sticky roles database with %d members",
        members_to_store,
    )


@plugin.listen(hikari.MemberDeleteEvent)
async def on_member_delete(event: hikari.MemberDeleteEvent) -> None:
    state = get_state()
    member = event.old_member
    if member is None:
        user = event.user
        if isinstance(user, hikari.Member):
            member = user
    if member is None:
        return

    role_ids = extract_member_roles(member)
    if not role_ids:
        return

    try:
        await update_sticky_roles(state, int(member.id), role_ids)
    except Exception:
        logger.exception("Failed to save sticky roles for leaving member %s", member.id)


@plugin.listen(hikari.MemberCreateEvent)
async def on_member_create(event: hikari.MemberCreateEvent) -> None:
    state = get_state()
    member = event.member

    try:
        sticky_record = StickyRoleDB.get_member(env, int(member.id))
        sticky_role_ids = sticky_record.get("role_ids", []) if sticky_record else []
        if not sticky_role_ids:
            return

        guild = await state.app.rest.fetch_guild(member.guild_id)
        if guild is None:
            logger.info(
                "Failed to find guild while restoring roles for member %s",
                member.id,
            )
            return

        roles_to_add = _filter_sticky_restorable_roles(guild, sticky_role_ids)
        if not roles_to_add:
            return

        for role in roles_to_add:
            await member.add_role(role)

        await update_sticky_roles(
            state,
            int(member.id),
            [int(role.id) for role in roles_to_add],
        )
        logger.info(
            "Restored %d sticky roles for member %s",
            len(roles_to_add),
            member.id,
        )
    except Exception:
        logger.exception("Failed to restore sticky roles for member %s", member.id)


@plugin.listen(hikari.MemberUpdateEvent)
async def on_member_update(event: hikari.MemberUpdateEvent) -> None:
    state = get_state()
    member = event.member
    old_member = event.old_member

    before_roles = set(map(int, old_member.role_ids)) if old_member else set()
    after_roles = set(map(int, member.role_ids))
    if before_roles == after_roles:
        return

    try:
        await update_sticky_roles(state, int(member.id), list(after_roles))
        await divide_member_roles(state, member)
    except Exception:
        logger.exception("Failed to update sticky roles for member %s", member.id)


@contextlib.asynccontextmanager
async def member_lock(state: RolesState, member_id: int) -> AsyncGenerator[None, None]:
    now = _utcnow()
    lock_data = state.member_role_locks.get(member_id)
    if lock_data is None:
        new_lock_data: _LockData = {"lock": asyncio.Lock(), "last_used": now}
        state.member_role_locks[member_id] = new_lock_data
        lock_data = new_lock_data
    else:
        lock_data["last_used"] = now

    lock = lock_data["lock"]
    async with lock:
        yield

    if lock.locked():
        return

    cutoff = now - datetime.timedelta(hours=1)
    state.member_role_locks = {
        mid: data
        for mid, data in state.member_role_locks.items()
        if data["last_used"] > cutoff
    }


async def _update_member_roles(
    member: hikari.Member,
    *,
    add_role: hikari.Role,
    remove_role: hikari.Role,
    current_roles: frozenset[int],
) -> bool:
    needs_add = int(add_role.id) not in current_roles
    needs_remove = int(remove_role.id) in current_roles

    if not needs_add and not needs_remove:
        return True

    try:
        if needs_remove:
            await member.remove_role(remove_role)
        if needs_add:
            await member.add_role(add_role)

        refreshed = await member.fetch_self()
        if refreshed is None:
            return False

        refreshed_roles = {int(role_id) for role_id in refreshed.role_ids}
        return (
            int(add_role.id) in refreshed_roles
            and int(remove_role.id) not in refreshed_roles
        )
    except Exception:
        logger.exception("Failed to update role state for member %s", member.id)
        return False


async def process_approval(
    ctx: miru.abc.Context,
    state: RolesState,
    member: hikari.Member,
    roles: dict[str, hikari.Role | None],
    current_roles: frozenset[int],
    thread_approvals: Approval,
    thread: hikari.GuildPublicThread,
) -> str:
    is_appr_forum = thread.parent_id == APPR_VETTING_FORUM_ID
    required_role = roles["required"]
    target_role = roles["target"]
    target_name = "approved" if is_appr_forum else "electoral"
    required_name = "temporary" if is_appr_forum else "approved"

    if target_role is None or int(target_role.id) in current_roles:
        await reply_err(
            state.app,
            ctx,
            f"Failed to approve {member.mention} - already has {target_name} role",
        )
        return f"Failed to approve - already has {target_name} role"

    if required_role is None or int(required_role.id) not in current_roles:
        await reply_err(
            state.app,
            ctx,
            f"Failed to approve {member.mention} - missing {required_name} role",
        )
        return f"Failed to approve - missing {required_name} role"

    required_approvals = 1 if is_appr_forum else REQUIRED_APPROVALS
    updated_approvals = Approval(
        approval_count=min(thread_approvals.approval_count + 1, required_approvals),
        rejection_count=thread_approvals.rejection_count,
        reviewers=thread_approvals.reviewers | {int(ctx.user.id)},
        last_approval_time=thread_approvals.last_approval_time,
    )
    state.approval_counts[int(thread.id)] = updated_approvals

    if updated_approvals.approval_count != required_approvals:
        remaining = required_approvals - updated_approvals.approval_count
        await reply_ok(
            state.app,
            ctx,
            (
                f"Registering approval for {member.mention}. "
                f"Status: {updated_approvals.approval_count}/{required_approvals} approvals. "
                f"Waiting for {remaining} more approvals.",
            ),
        )
        return "Registering approval"

    final_approval = dataclasses.replace(
        updated_approvals,
        last_approval_time=_utcnow(),
    )
    state.approval_counts[int(thread.id)] = final_approval
    if required_role is None or target_role is None:
        await reply_err(state.app, ctx, "Failed to resolve required roles")
        return "Failed to approve - role resolution failed"

    updated = await _update_member_roles(
        member,
        add_role=target_role,
        remove_role=required_role,
        current_roles=current_roles,
    )
    if not updated:
        await reply_err(
            state.app,
            ctx,
            f"Failed to approve {member.mention} due to role update failure",
        )
        return "Failed to approve - role update failed"

    await reply_ok(
        state.app,
        ctx,
        f"Approved {member.mention} by {_format_reviewers(final_approval.reviewers)}.",
        ephemeral=False,
    )
    state.approval_counts.pop(int(thread.id), None)
    state.processed_thread_ids.discard(int(thread.id))
    return f"Approved {member.mention} with role updates"


async def process_rejection(
    ctx: miru.abc.Context,
    state: RolesState,
    member: hikari.Member,
    roles: dict[str, hikari.Role | None],
    current_roles: frozenset[int],
    thread_approvals: Approval,
    thread: hikari.GuildPublicThread,
) -> str:
    is_appr_forum = thread.parent_id == APPR_VETTING_FORUM_ID
    required_role = roles["required"]
    fallback_role = roles["fallback"]
    required_name = "temporary" if is_appr_forum else "approved"

    if required_role is None or int(required_role.id) not in current_roles:
        await reply_err(
            state.app,
            ctx,
            f"Failed to reject {member.mention} - has not been granted {required_name} role",
        )
        return f"Failed to reject - no {required_name} role"

    if (
        not is_appr_forum
        and thread_approvals.last_approval_time is not None
        and (_utcnow() - thread_approvals.last_approval_time).total_seconds()
        > REJECTION_WINDOW_DAYS * 86400
    ):
        await reply_err(
            state.app,
            ctx,
            (
                f"Failed to reject {member.mention} - rejection window expired after {REJECTION_WINDOW_DAYS} days"
            ),
        )
        return "Failed to reject - rejection window closed"

    required_rejections = 1 if is_appr_forum else REQUIRED_REJECTIONS
    updated_approvals = Approval(
        approval_count=thread_approvals.approval_count,
        rejection_count=min(thread_approvals.rejection_count + 1, required_rejections),
        reviewers=thread_approvals.reviewers | {int(ctx.user.id)},
        last_approval_time=thread_approvals.last_approval_time,
    )
    state.approval_counts[int(thread.id)] = updated_approvals

    if updated_approvals.rejection_count != required_rejections:
        remaining = required_rejections - updated_approvals.rejection_count
        await reply_ok(
            state.app,
            ctx,
            (
                f"Registering rejection for {member.mention}. "
                f"Status: {updated_approvals.rejection_count}/{required_rejections} rejections. "
                f"Waiting for {remaining} more rejections.",
            ),
        )
        return "Registering rejection"

    if fallback_role is None:
        await reply_err(state.app, ctx, "Failed to resolve fallback role")
        return "Failed to reject - role resolution failed"

    updated = await _update_member_roles(
        member,
        add_role=fallback_role,
        remove_role=required_role,
        current_roles=current_roles,
    )
    if not updated:
        await reply_err(
            state.app,
            ctx,
            f"Failed to reject {member.mention} due to role update failure",
        )
        return "Failed to reject - role update failed"

    await reply_ok(
        state.app,
        ctx,
        f"Rejected {member.mention} by {_format_reviewers(updated_approvals.reviewers)}.",
        ephemeral=False,
    )
    state.approval_counts.pop(int(thread.id), None)
    state.processed_thread_ids.discard(int(thread.id))
    return f"Rejected {member.mention} with role updates"


async def process_status(
    ctx: miru.abc.Context,
    state: RolesState,
    status: str,
) -> str | None:
    channel = await state.app.rest.fetch_channel(ctx.channel_id)
    if not isinstance(channel, hikari.GuildPublicThread):
        raise ValueError("Failed to detect thread context")

    guild = await state.app.rest.fetch_guild(channel.guild_id)
    if guild is None:
        await reply_err(state.app, ctx, "Failed to find guild")
        return None

    if channel.owner_id is None:
        await reply_err(state.app, ctx, "Failed to find thread owner")
        return None

    member = await state.app.rest.fetch_member(channel.guild_id, channel.owner_id)
    if member is None:
        await reply_err(state.app, ctx, "Failed to find thread owner")
        return None

    async with member_lock(state, int(member.id)):
        thread_approvals = state.approval_counts.get(int(channel.id), Approval())
        if int(ctx.user.id) in thread_approvals.reviewers:
            await reply_err(state.app, ctx, "Failed to detect duplicate vote")
            return None

        is_appr_forum = channel.parent_id == APPR_VETTING_FORUM_ID
        roles = _resolve_workflow_roles(guild, is_appr_forum=is_appr_forum)
        if (
            roles["required"] is None
            or roles["target"] is None
            or roles["fallback"] is None
        ):
            await reply_err(
                state.app,
                ctx,
                "Failed to validate role configuration",
            )
            return None

        current_roles = frozenset(int(role_id) for role_id in member.role_ids)

        if status == "approved":
            result = await process_approval(
                ctx=ctx,
                state=state,
                member=member,
                roles=roles,
                current_roles=current_roles,
                thread_approvals=thread_approvals,
                thread=channel,
            )
        elif status == "rejected":
            result = await process_rejection(
                ctx=ctx,
                state=state,
                member=member,
                roles=roles,
                current_roles=current_roles,
                thread_approvals=thread_approvals,
                thread=channel,
            )
        else:
            await reply_err(state.app, ctx, "Failed to validate status")
            return None

    approval_info = state.approval_counts.get(int(channel.id), Approval())
    embed = await _build_vote_embed(
        state,
        guild=guild,
        approval=approval_info,
        is_appr_forum=channel.parent_id == APPR_VETTING_FORUM_ID,
    )

    if isinstance(ctx, miru.ViewContext) and ctx.message is not None:
        try:
            await ctx.edit_response(
                embed=embed,
                components=ctx.view.build()
                if ctx.view is not None
                else hikari.UNDEFINED,
            )
        except Exception:
            logger.exception(
                "Failed to update approval message for thread %s",
                channel.id,
            )

    return result


@plugin.listen(hikari.GuildThreadCreateEvent)
async def on_thread_create(event: hikari.GuildThreadCreateEvent) -> None:
    state = get_state()
    thread = event.thread
    if not isinstance(thread, hikari.GuildPublicThread):
        return

    thread_id = int(thread.id)
    if thread.parent_id not in (ELECT_VETTING_FORUM_ID, APPR_VETTING_FORUM_ID):
        return
    if thread_id in state.processed_thread_ids:
        return
    if thread.owner_id is None:
        return

    try:
        now = _utcnow()
        await thread.edit(name=f"[{now.strftime('%y%m%d%H%M')}] {thread.name}")

        guild = await state.app.rest.fetch_guild(GUILD_ID)
        approval = state.approval_counts.get(thread_id, Approval())
        embed = await _build_vote_embed(
            state,
            guild=guild,
            approval=approval,
            is_appr_forum=thread.parent_id == APPR_VETTING_FORUM_ID,
        )
        view = ApprovalView(state)
        message = await thread.send(embed=embed, components=view.build())
        state.miru_client.start_view(view, bind_to=message)
    except Exception:
        logger.exception("Failed to process new thread %s", thread_id)
    finally:
        state.processed_thread_ids.add(thread_id)


@group_servant.include()
@arc.slash_subcommand("view", description="Servant Directory")
async def cmd_servant_view(ctx: arc.GatewayContext) -> None:
    state = get_state()
    await defer(ctx)

    guild = await state.app.rest.fetch_guild(GUILD_ID)
    if guild is None:
        await reply_err(state.app, ctx, "Failed to find guild")
        return

    all_roles = guild.get_roles()
    if not all_roles:
        await reply_err(state.app, ctx, "Failed to find roles")
        return

    sorted_roles = sorted(
        all_roles.values(),
        key=lambda role: role.position,
        reverse=True,
    )
    divider_name = "═════･[Bot身份组]･═════"
    divider_idx = next(
        (index for index, role in enumerate(sorted_roles) if role.name == divider_name),
        len(sorted_roles),
    )

    filtered_roles = tuple(
        role
        for role in sorted_roles[:divider_idx]
        if not role.name.startswith(("——", "══"))
        and (role.bot_id is None or role.is_managed)
    )
    if not filtered_roles:
        await reply_err(state.app, ctx, "Failed to find matching roles")
        return

    role_to_members: dict[int, list[str]] = {
        int(role.id): [] for role in filtered_roles
    }
    async for member in state.app.rest.fetch_members(GUILD_ID):
        for role_id in member.role_ids:
            role_members = role_to_members.get(int(role_id))
            if role_members is not None:
                role_members.append(member.mention)

    role_members_list = [
        {
            "role_name": role.name,
            "members": mentions,
            "member_count": len(mentions),
        }
        for role in filtered_roles
        if (mentions := role_to_members[int(role.id)])
    ]
    if not role_members_list:
        await reply_err(state.app, ctx, "Failed to find matching roles")
        return

    total_members = sum(item["member_count"] for item in role_members_list)
    title = f"Servant Directory ({total_members} members)"

    embeds: list[hikari.Embed] = []
    current_embed = await reply_embed(state.app, title=title, ctx=ctx)
    field_count = 0

    for role_data in role_members_list:
        if field_count >= 25:
            embeds.append(current_embed)
            current_embed = await reply_embed(state.app, title=title, ctx=ctx)
            field_count = 0

        current_embed.add_field(
            name=f"{role_data['role_name']} ({role_data['member_count']} members)",
            value="\n".join(role_data["members"]),
            inline=True,
        )
        field_count += 1

    if field_count:
        embeds.append(current_embed)

    buttons: list[nav.NavItem] = [
        nav.PrevButton(),
        nav.StopButton(),
        nav.NextButton(),
    ]
    navigator = nav.navigator.NavigatorView(
        pages=embeds,
        items=buttons,
        timeout=180,
        autodefer=True,
    )
    builder = await navigator.build_response_async(state.miru_client)
    await respond_with_builder_and_bind_view(
        ctx=ctx,
        builder=builder,
        miru_client=state.miru_client,
        view=navigator,
    )


async def _restore_persistent_views(state: RolesState) -> None:
    try:
        forums = (ELECT_VETTING_FORUM_ID, APPR_VETTING_FORUM_ID)
        restored_count = 0

        for forum_id in forums:
            try:
                threads_result = await state.app.rest.fetch_active_threads(GUILD_ID)
                for thread in threads_result:
                    if not isinstance(thread, hikari.GuildPublicThread):
                        continue
                    if thread.parent_id != forum_id:
                        continue

                    try:
                        async for message in state.app.rest.fetch_messages(thread.id):
                            if not message.components:
                                continue
                            for row in message.components:
                                if not isinstance(row, hikari.ActionRowComponent):
                                    continue
                                for component in row.components:
                                    if isinstance(
                                        component,
                                        (
                                            hikari.ButtonComponent,
                                            hikari.SelectMenuComponent,
                                        ),
                                    ):
                                        cid = component.custom_id
                                        if cid in ("approve", "reject"):
                                            view = ApprovalView(state)
                                            state.miru_client.start_view(
                                                view,
                                                bind_to=message,
                                            )
                                            restored_count += 1
                                            break
                                else:
                                    continue
                                break
                    except Exception:
                        logger.exception(
                            "Failed to scan thread %s for approval buttons",
                            thread.id,
                        )
            except Exception:
                logger.exception("Failed to fetch threads from forum %s", forum_id)

        if restored_count > 0:
            logger.info("Restored %d persistent approval views", restored_count)
    except Exception:
        logger.exception("Failed to restore persistent views")


@plugin.listen(hikari.StartedEvent)
async def on_roles_started(_: hikari.StartedEvent) -> None:
    state = get_state()
    await initial_data(state)
    await _restore_persistent_views(state)


@arc.loader
def load(client: arc.GatewayClient) -> None:
    global _state
    if _state is not None:
        return

    database.open()
    global env
    env = database.env
    state = RolesState(client=client)
    _state = state
    client.add_plugin(plugin)


@arc.unloader
def unload(client: arc.GatewayClient) -> None:
    global _state
    global env
    client.remove_plugin(plugin)

    state = _state
    if state is None:
        return

    database.close()
    env = None
    _state = None


class ApprovalView(miru.View):
    def __init__(self, state: RolesState) -> None:
        super().__init__(timeout=None)
        self.state = state

    @miru.button(style=hikari.ButtonStyle.SUCCESS, label="Approve", custom_id="approve")
    async def approve_btn(self, ctx: miru.ViewContext, _: miru.Button) -> None:
        await process_status(ctx, self.state, "approved")

    @miru.button(style=hikari.ButtonStyle.DANGER, label="Reject", custom_id="reject")
    async def reject_btn(self, ctx: miru.ViewContext, _: miru.Button) -> None:
        await process_status(ctx, self.state, "rejected")
