import redis
from redis import client
from typing import Union, List, Any
from collections.abc import MutableMapping

from py_redis_client.constants import (
    ExpiryType, CacheDataType, LIST, SET, HASHMAP,
    ADDRESS, LIST_SEP, SET_SEP, CONVERT, UNCONVERT)
from py_redis_client.conversions import Conversions
from py_redis_client.db import RedisNative, RedisList, RedisSet, RedisHashMap
from py_redis_client.db.base import _RedisDB
from py_redis_client.exceptions import InavlidRedisKeyError, InavlidRedisValueError
from py_redis_client.pipe_execution import Operation, PipeExecution


class DBExecutions(PipeExecution):
    def set_in_db(
            self, data: dict,
            expiry: ExpiryType = None) -> None:
        self.clear_operations
        native_kwargs = {
            "data": {}, "expiry": expiry}
        
        def list_operation(
                key, value, address = True):
            if address:
                native_kwargs["data"]["{}${}".format(
                        key, ADDRESS)] = LIST
            self.add_operation(Operation(
                RedisList, "set", kwargs={
                    "key": key, "expiry": expiry,
                    "data": value}))
        
        def set_operation(
                key, value, address = True):
            if address:
                native_kwargs["data"]["{}${}".format(
                        key, ADDRESS)] = SET
            self.add_operation(Operation(
                RedisSet, "set", kwargs={
                    "key": key, "expiry": expiry,
                    "data": value}))
        
        def hmap_operation(key, data):
            native_kwargs["data"]["{}${}".format(
                        key, ADDRESS)] = HASHMAP
            self.add_operation(Operation(
                RedisHashMap, "set", kwargs={
                    "key": key, "data": data,
                    "expiry": expiry}))

        self.add_operation(Operation(
            _RedisDB, "db_multi"))
        for key, value in data.items():
            if isinstance(value, tuple):
                value = list(value)
            if isinstance(value, list) and value:
                list_operation(key, value)
            elif isinstance(value, set) and value:
                set_operation(key, value)
            elif isinstance(value, MutableMapping) and value:
                res = {}
                lists = []
                sets = []
                for k, v in value.items():
                    if isinstance(v, list) and v:
                        lists.append(k)
                        list_operation(
                            key + "$" + k, v, False)
                    elif isinstance(v, set) and v:
                        sets.append(k)
                        set_operation(
                            key + "$" + k, v, False)
                    elif v is not None:
                        res[k] = v
                if res:
                    hmap_operation(key, res)
                if lists:
                    native_kwargs["data"]["{}${}".format(
                        key, LIST)] = "$".join(lists)
                if sets:
                    native_kwargs["data"]["{}${}".format(
                        key, SET)] = "$".join(sets)
            elif value is not None:
                native_kwargs["data"][key] = value
        if native_kwargs["data"]:
            self.add_operation(Operation(
                RedisNative, "set_many", kwargs=native_kwargs))
        self.execute
    
    def get_from_db(self, *keys):
        self.clear_operations
        get_address = [
            "{}${}".format(key, suffix) for key in
            keys for suffix in [ADDRESS, LIST, SET]]
        address_map = RedisNative(self.redis).get_many(
            *get_address)
        
        def add_hash_iterables(key, iterables = [LIST, SET]):
            klass_map = {LIST: RedisList, SET: RedisSet}
            iters_keys = []
            iter_map = {}
            for itr in iterables:
                iters_keys.append(address_map.get(
                    "{}${}".format(key, itr), "").split("$"))
            for idx, iter_keys in enumerate(iters_keys):
                klass = klass_map.get(iterables[idx])
                if iter_keys == [""] or klass is None:
                    continue
                for iter_key in iter_keys:
                    created_key = key + "$" + iter_key
                    self.add_operation(Operation(
                        klass, "execute_get", [created_key],
                        kwargs={"key": created_key}))
                iter_map[iterables[idx]] = iter_keys
            return iter_map
        
        def get_values(
                data: dict, hash_iterables: dict,
                native_keys: List[str]):
            list_val, set_val, hm_val, nat_val = (
                RedisList(self.redis).format_get,
                RedisSet(self.redis).format_get,
                RedisHashMap(self.redis).format_get,
                RedisNative(self.redis).format_get_many)
            native_val = data.pop("$native", [])
            res = {}
            for key, value in data.items():
                if not value:
                    continue
                address = address_map.get(
                    "{}${}".format(key, ADDRESS))
                if address == LIST:
                    res[key] = list_val(*value)
                elif address == SET:
                    res[key] = set_val(*value)
                elif address == HASHMAP:
                    temp = {}
                    hmap = {}
                    for k, v in value.items():
                        if not v:
                            continue
                        if k in hash_iterables.get(
                            key, {}).get(LIST, []):
                            hmap[k] = list_val(*v)
                        elif k in hash_iterables.get(
                            key, {}).get(SET, []):
                            hmap[k] = set_val(*v)
                        else:
                            temp[k] = v
                    hmap.update(hm_val(temp))
                    if hmap:
                        res[key] = hmap
            if native_keys:
                res.update(nat_val(native_keys, native_val))
            return res

        native_keys = []
        hm_iterables = {}
        for key in keys:
            address = address_map.get(
                "{}${}".format(key, ADDRESS))
            if address == LIST:
                self.add_operation(Operation(
                    RedisList, "execute_get", [key], kwargs={"key": key}))
            elif address == SET:
                self.add_operation(Operation(
                    RedisSet, "execute_get", [key], kwargs={"key": key}))
            elif address == HASHMAP:
                self.add_operation(Operation(
                    RedisHashMap, "execute_get", [key], kwargs={"key": key}))
                hm_iterables[key] = add_hash_iterables(key)
            else:
                native_keys.extend([
                    key, "|" + LIST_SEP + "|" + key,
                    "|" + SET_SEP + "|" + key])
        if native_keys:
            self.add_operation(Operation(
                RedisNative, "execute_get_many",
                ["$native"], args=native_keys))
        result = self.execute
        formatted_data = {}
        for k, v in result.items():
            if "$" in k and k != "$native":
                temp = k.split("$")
                hm = formatted_data.get(temp[0], {})
                hm.update({temp[1]: v})
                formatted_data[temp[0]] = hm
            else:
                if k in formatted_data:
                    formatted_data[k].update(v)
                else:
                    formatted_data[k] = v
        return get_values(
            formatted_data, hm_iterables, native_keys)


class Mapper:
    @staticmethod
    def map_to_db(
        redis_conn: redis.Redis,
        data: dict, expiry: ExpiryType = None,
        separator: Union[str, None] = None) -> None:
        conv = Conversions(CONVERT)

        def separator_iterable(
                key: str, value: Any, res: dict):
            if (separator is not None
                and (isinstance(value, list)
                     or isinstance(value, set))):
                iter_type = LIST_SEP if isinstance(
                    value, list) else SET_SEP
                formatted_value = []
                for val in value:
                    formatted_val = conv.final_value(val)
                    if separator in formatted_val:
                        raise InavlidRedisValueError(
                            "Iterable set with a separator that is"
                            " included in formatted value. Separator"
                            " - {}, Formatted val - {}".format(
                                separator, formatted_val))
                    formatted_value.append(formatted_val)
                res["|" + iter_type + "|" + key] = (
                    separator + "|" + "{}".format(
                        separator).join(formatted_value))
            else:
                res[key]= value

        def flatten_hmap(
                hmap: dict, parent_key: str = "",
                res: dict = {}):
            for k, v in hmap.items():
                k = conv.final_value(k)
                if "$" in k or "|" in k:
                    raise InavlidRedisKeyError(
                        "Cache keys cannot contain $ or |")
                if isinstance(v, tuple):
                    v = list(v)
                if isinstance(v, MutableMapping):
                    flatten_hmap(
                        v, parent_key + "|" + k if
                        parent_key else k, res)
                else:
                    key = (parent_key + "|" + k
                           if parent_key else k)
                    separator_iterable(key, v, res)

        to_map = {}
        for k, v in data.items():
            conv.key_validate(k)
            if "$" in k or "|" in k:
                raise InavlidRedisKeyError(
                    "Cache keys cannot contain $ or |")
            if isinstance(v, tuple):
                v = list(v)
            if isinstance(v, MutableMapping):
                res = {}
                flatten_hmap(v, res=res)
                to_map[k] = res
            else:
                separator_iterable(k, v, to_map)

        DBExecutions(redis_conn).set_in_db(
            to_map, expiry)
    
    @staticmethod
    def unmap_from_db(
        redis_conn: client.Pipeline,
        *keys) -> dict[str: CacheDataType]:
        unconv = Conversions(UNCONVERT, False)
        
        def deseparate_iterable(
                key: str, value: Any):
            if key.startswith("|" + LIST_SEP) or key.startswith(
                "|" + SET_SEP):
                _, iter_type, original_key = key.split(
                    "|", maxsplit=2)
                sep, value = value.split(
                    "|", maxsplit=1)
                return original_key, (
                    [unconv.final_value(e) for e in value.split(sep)]
                    if iter_type == LIST_SEP else
                    {unconv.final_value(e) for e in value.split(sep)})
            return key, value

        def unflatten_hmap(
                key: str, value: Any, res: dict):
            separated_keys = key.split(
                "|", maxsplit=1)
            curr_key = unconv.final_value(
                separated_keys[0])
            if len(separated_keys) > 1:
                if curr_key not in res:
                    res[curr_key] = {}
                unflatten_hmap(
                    separated_keys[1], value,
                    res[curr_key])
            else:
                res[curr_key] = value

        for key in keys:
            unconv.key_validate(key)
        result = DBExecutions(redis_conn).get_from_db(
            *keys)
        to_return = {}
        for k, v in result.items():
            k, v = deseparate_iterable(k, v)
            if isinstance(v, MutableMapping):
                new_dict = {}
                for x, y in v.items():
                    x, y = deseparate_iterable(x, y)
                    unflatten_hmap(x, y, new_dict)
                to_return[k] = new_dict
            else:
                to_return[k] = v
        return to_return

    @staticmethod
    def delete_from_db(redis_conn: redis.Redis, *keys: str) -> None:
        keys_to_delete = []
        for key in keys:
            keys_to_delete.extend([
                key,
                "|" + LIST_SEP + "|" + key,
                "|" + SET_SEP + "|" + key])
        _RedisDB(redis_conn).delete(*keys_to_delete)
