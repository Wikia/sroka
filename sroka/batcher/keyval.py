from collections import defaultdict
from functools import cached_property
from typing import Any, Literal, Union, Dict, Tuple, Set
from .gam_helpers import statement_iter
import googleads.ad_manager as ad_manager
from zeep.helpers import serialize_object
from shelve import open as shopen
from datetime import datetime, timedelta, timezone
import atexit

SHELF_EXPIRATION = timedelta(weeks=2)


class NotAKeyValException(Exception):
    @classmethod
    def incorrect_operator(cls, operator):
        return cls(f"Operator {operator} is not a valid operator.")


class Macro(object):
    DEFINED = None

    def __init__(self, call, name) -> None:
        self.callable = call
        self.name = name

    def __call__(self, container, contained, **kwds) -> bool:
        return self.callable(container, contained, **kwds)

    def __str__(self) -> str:
        return f"MACRO({self.name})"


Macro.DEFINED = Macro(lambda x, y: True, "exists in the set")


class hashabledict(dict):
    def __hash__(self):
        return hash(frozenset(self.items()))


SHELF = None
shelf_n = 0
while SHELF is None:
    try:
        SHELF = shopen(f".keyvalcache-{shelf_n}")
    except:
        pass
    shelf_n += 1

if (
    len(SHELF) == 0
    or "expiration date" not in SHELF
    or datetime.fromtimestamp(SHELF["expiration date"] or 0, timezone.utc)
    < datetime.now(timezone.utc)
):
    SHELF["expiration date"] = (
        datetime.now(timezone.utc) + SHELF_EXPIRATION
    ).timestamp()
    SHELF["keys"] = {}
    SHELF["values"] = {}


class Targeting:
    key_cache: Dict[Union[int, str], Any] = SHELF["keys"]
    val_cache: Dict[Tuple[int, Union[int, str]], Any] = SHELF["values"]
    keys_to_get_name: Set[str] = set()
    keys_to_get_id: Set[int] = set()
    vals_to_get_name: Set[str] = set()
    vals_to_get_id: Set[int] = set()

    IN_MODE: Literal["contains", "containsExclusive"] = "containsExclusive"

    def __init__(self, *args, **kwds):
        if kwds.get("string_override"):
            self.string_override = (
                f"--start override--\n{kwds['string_override']}\n--end override--"
            )
            del kwds["string_override"]
        else:
            self.string_override = None
        self.macro = None
        self.source = dict(*args, **kwds)
        if kwds.get("key") or kwds.get("keyId"):
            self._prepare_key(kwds.get("key") or kwds.get("keyId"))

        values = kwds.get("values") or kwds.get("valueIds") or set()
        for i in values:
            self._prepare_val(i)

    def __setitem__(self, key, value):
        if key in {"key", "keyId"}:
            self._prepare_key(value)

        elif key in {"values", "valueIds"}:
            for i in value:
                self._prepare_val(i)
        self.source.__setitem__(key, value)

    def __getitem__(self, key):
        return self.source.__getitem__(key)

    @classmethod
    def _prepare_key(cls, key: Union[int, str]):
        if key in cls.key_cache:
            return
        if isinstance(key, str):
            cls.keys_to_get_name.add(key)
        else:
            cls.keys_to_get_id.add(key)

    @classmethod
    def _prepare_val(cls, val: Union[int, str]):
        if val in {i for _, i in cls.val_cache}:
            return
        if isinstance(val, str):
            cls.vals_to_get_name.add(val)
        else:
            cls.vals_to_get_id.add(val)

    @classmethod
    def _update_keys(cls):
        if len(cls.keys_to_get_id) > 0:
            statement = ad_manager.StatementBuilder().Where(
                f"id IN ({', '.join((str(i) for i in cls.keys_to_get_id))})"
            )
            buff = tuple(statement_iter("CustomTargetingService", statement, "Keys"))
            cls.key_cache.update({i.id: serialize_object(i, dict) for i in buff})
            cls.key_cache.update({i.name: serialize_object(i, dict) for i in buff})
            cls.keys_to_get_id = set()

        if len(cls.keys_to_get_name) > 0:
            names = "', '".join(cls.keys_to_get_name)
            statement = ad_manager.StatementBuilder().Where(f"name IN ('{names}')")
            buff = tuple(statement_iter("CustomTargetingService", statement, "Keys"))
            cls.key_cache.update({i.id: serialize_object(i, dict) for i in buff})
            cls.key_cache.update({i.name: serialize_object(i, dict) for i in buff})
            cls.keys_to_get_name = set()

    @classmethod
    def _update_vals(cls):
        keys = ", ".join({str(i) for i in cls.key_cache if isinstance(i, int)})
        if len(cls.vals_to_get_id) > 0:
            statement = ad_manager.StatementBuilder().Where(
                f"id IN ({', '.join((str(i) for i in cls.vals_to_get_id))}) AND customTargetingKeyId IN ({keys})"
            )
            buff = tuple(statement_iter("CustomTargetingService", statement, "Values"))
            cls.val_cache.update(
                {
                    (i.customTargetingKeyId, i.id): serialize_object(i, dict)
                    for i in buff
                }
            )
            cls.val_cache.update(
                {
                    (i.customTargetingKeyId, i.name): serialize_object(i, dict)
                    for i in buff
                }
            )
            cls.vals_to_get_id = set()

        if len(cls.vals_to_get_name) > 0:
            names = "', '".join(cls.vals_to_get_name)
            statement = ad_manager.StatementBuilder().Where(
                f"name IN ('{names}') AND customTargetingKeyId IN ({keys})"
            )
            buff = tuple(statement_iter("CustomTargetingService", statement, "Values"))
            cls.val_cache.update(
                {
                    (i.customTargetingKeyId, i.id): serialize_object(i, dict)
                    for i in buff
                }
            )
            cls.val_cache.update(
                {
                    (i.customTargetingKeyId, i.name): serialize_object(i, dict)
                    for i in buff
                }
            )
            cls.vals_to_get_name = set()

    @cached_property
    def logicalOperator(self):
        return self.source.get("logicalOperator")

    @cached_property
    def key(self):
        self._update_keys()
        return self.key_cache.get(
            self.source.get("key") or self.source.get("keyId"),
            defaultdict(lambda: None),
        )

    @cached_property
    def vals(self):
        self._update_keys()
        self._update_vals()
        keyId = self.key["id"]
        return [
            self.val_cache.get((keyId, val), defaultdict(lambda: None))
            for val in self.source.get("values", set()).union(
                self.source.get("valueIds", set())
            )
        ]

    def set_macro(self, val):
        self.macro = val

    @classmethod
    def from_strings(cls, key: str, operator: str, vals: Set[str]):
        assert isinstance(key, str) and all(isinstance(i, str) for i in vals)
        return cls(key=key, operator=operator, values=vals)

    @classmethod
    def from_string(cls, text: str):
        text = text.translate({"\n": None})
        ors = [cls._from_and(i) for i in text.split(" OR ")]
        return OR(*ors)

    @classmethod
    def _from_and(cls, text: str):
        predicates = []
        for predicate in text.split(" AND "):
            if "!=" in predicate:
                key, vals_str = predicate.split("!=")
                vals = vals_str.split(",")
                predicates.append(IS_NOT(key.strip(), {i.strip() for i in vals}))
            elif "=" in predicate:
                key, vals_str = predicate.split("=")
                vals = vals_str.split(",")
                predicates.append(IS(key.strip(), {i.strip() for i in vals}))
            else:
                raise NotImplementedError("Predicates can only contain != or =")
        return AND(*predicates)

    @cached_property
    def in_strings(self):
        if "IS" not in self.type_:
            return hashabledict(
                logicalOperator=self.source.get("logicalOperator", "UNKNOWN"),
                children=[i.in_strings for i in self.source.get("children", [])],
            )
        return hashabledict(
            key=self.key["name"],
            operator=self.source["operator"],
            values={i["name"] for i in self.vals},
        )

    @cached_property
    def in_string(self):
        if self.string_override is not None:
            return self.string_override
        if self.source.get("logicalOperator") == "OR":
            return "\nOR\n".join(i.in_string for i in self.source["children"])
        elif self.source.get("logicalOperator") == "AND":
            return " AND\n".join(sorted(i.in_string for i in self.source["children"]))
        else:
            operator = "in" if self.source["operator"] == "IS" else "not in"
            return f"{self.key['name']} {operator} ({', '.join(sorted(i['name'] for i in self.vals))})"

    @cached_property
    def comparable(self):
        if self.type_ in ("AND", "OR"):
            return hashabledict(
                logicalOperator=self.source["logicalOperator"],
                children=[i.comparable for i in self.source["children"]],
            )
        elif self.type_ in ("IS", "IS_NOT"):
            return hashabledict(
                keyId=self.key["id"],
                operator=self.source["operator"],
                valueIds={i["id"] for i in self.vals},
            )
        else:
            raise NotAKeyValException.incorrect_operator(self.type_)

    @cached_property
    def type_(self):
        return self.source.get("operator") or self.source.get("logicalOperator")

    @cached_property
    def _toplevel(self):
        obj = self
        if obj.type_ not in ("AND", "OR"):
            obj = AND(obj)
        if obj.type_ != "OR":
            obj = OR(obj)
        return obj

    # Other constructors

    @classmethod
    def from_line(cls, line_item_id: int):
        s = (
            ad_manager.StatementBuilder()
            .Where("Id = :id")
            .WithBindVariable("id", line_item_id)
        )
        return cls.from_results(next(statement_iter("LineItemService", s)))

    def search_similar(self, pql=None):
        """
        This only works for the non-advanced targeting
        """
        s = pql or ad_manager.StatementBuilder()
        for line in statement_iter("LineItemService", s):
            other = self.from_results(line)
            if other is not None and other.contains(self):
                yield line

    @classmethod
    def from_results(cls, result):
        return serialize_object(result['targeting']['customTargeting'], cls)

    def __eq__(self, o: object) -> bool:
        if isinstance(o, self.__class__):
            return self.comparable == o.comparable
        return super().__eq__(o)

    def __contains__(self, o: object) -> bool:
        if isinstance(o, self.__class__):
            if self.IN_MODE == "contains":
                return self.contains(o)
            elif self.IN_MODE == "containsExclusive":
                return self.containsExclusive(o)

    def contains(self, o) -> bool:
        assert isinstance(o, self.__class__)
        assert self.type_ == "OR" and all(
            i.type_ == "AND" for i in self.source["children"]
        )
        return self._contains(o._toplevel)

    def containsExclusive(self, o) -> bool:
        assert isinstance(o, self.__class__)
        assert self.type_ == "OR" and all(
            i.type_ == "AND" for i in self.source["children"]
        )
        for i in self.source["children"]:
            return any(i._contains(j) for j in o._toplevel.source["children"])

    def _contains(self, o) -> bool:
        if self.key["id"] != o.key["id"] or self.logicalOperator != o.logicalOperator:
            return False

        elif self.type_ != o.type_:
            return False
        elif o.macro is not None:
            return o.macro(self, o)
        elif self.type_ in ("IS", "IS_NOT"):
            return self.comparable["valueIds"].issuperset(o.comparable["valueIds"])
        elif self.type_ in ("AND", "OR"):
            return all(
                any(j._contains(i) for j in self.source["children"])
                for i in o.source["children"]
            )
        else:
            raise NotAKeyValException.incorrect_operator(self.type_)


def save_shelf():
    SHELF["keys"] = Targeting.key_cache
    SHELF["values"] = Targeting.val_cache
    SHELF.close()


atexit.register(save_shelf)

# Tree leaves


def IS(key, vals, string_override=None):
    if isinstance(vals, Macro):
        r = Targeting.from_strings(key, "IS", set())
        r.string_override = string_override
        r.set_macro(vals)
        return r
    elif isinstance(vals, str):
        vals = {vals}
    elif hasattr(vals, "__iter__"):
        vals = set(vals)
    r = Targeting.from_strings(key, "IS", vals)
    r.string_override = string_override
    return r


def IS_NOT(key, vals, string_override=None):
    if isinstance(vals, Macro):
        r = Targeting.from_strings(key, "IS_NOT", set())
        r.string_override = string_override
        r.set_macro(vals)
        return r
    if isinstance(vals, str):
        vals = {vals}
    elif hasattr(vals, "__iter__"):
        vals = set(vals)
    r = Targeting.from_strings(key, "IS_NOT", vals)
    r.string_override = string_override
    return r


# Logic operators


def OR(*args, string_override=None):
    return Targeting(
        logicalOperator="OR", children=list(args), string_override=string_override
    )


def AND(*args, string_override=None):
    return Targeting(
        logicalOperator="AND", children=list(args), string_override=string_override
    )
