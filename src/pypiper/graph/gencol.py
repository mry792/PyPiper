"""."""

from collections.abc import Callable, Generator
from dataclasses import dataclass, field
from itertools import chain
from typing import Generic, TypeVar

T = TypeVar("T")
TGenCol = TypeVar("TGenCol", bound="GenCol")


class NonConsecutiveKeysError(ValueError):
    """Exception indicating that keys are not consecutive integers."""

    def __init__(self):
        """Initialize the exception."""
        super().__init__("Keys must be consecutive integers.")


class DuplicateKeysError(ValueError):
    """Exception indicating that keys are duplicated."""

    def __init__(self):
        """Initialize the exception."""
        super().__init__("Named keys must be unique.")


@dataclass
class GenCol(Generic[T]):
    """A generalized collection of sequenced and named values."""

    sequence: list[T] = field(default_factory=list)
    named: dict[str, T] = field(default_factory=dict)

    @classmethod
    def assemble(cls, key_values: dict[int | str, T]) -> "GenCol[T]":
        """Organize a `GenCol` from raw key-value pairs."""

        seq: list[tuple[int, T]] = []
        named: dict[str, T] = {}

        for key, value in key_values.items():
            if isinstance(key, str):
                named[key] = value
            else:
                seq.append((key, value))

        seq.sort()
        indices = [i for i, _ in seq]
        if indices != list(range(len(indices))):
            raise NonConsecutiveKeysError()

        return cls(
            sequence=[val for _, val in seq],
            named=named,
        )

    def get(self, key: int | str) -> T:
        """Lookup by arbitrary key."""
        if isinstance(key, str):
            return self.named[key]
        return self.sequence[key]

    def is_structural_match(self, other: "GenCol") -> bool:
        """Check if `other` conforms to the same structure as this."""

        if len(self.sequence) != len(other.sequence):
            return False
        if set(self.named.keys()) != set(other.named.keys()):
            return False
        return True

    def keys(self) -> Generator[int | str, None, None]:
        """Iterate over the keys of this collection."""
        yield from range(len(self.sequence))
        yield from self.named.keys()

    def values(self) -> Generator[T, None, None]:
        """Iterate over the values of this collection."""
        yield from self.sequence
        yield from self.named.values()

    def items(self) -> Generator[tuple[int | str, T], None, None]:
        """Iterate over all values and their keys."""

        yield from enumerate(self.sequence)
        yield from self.named.items()

    def invoke(self, func: Callable):
        """
        Invoke `func` using this collection's elements.

        Sequence elements are used as positional arguments while named
        elements are used as keyword arguments.

        :param func: The function to invoke.

        :returns: The result of `func`.
        """

        return func(*self.sequence, **self.named)

    def map(self, func: Callable, *args: "GenCol") -> "GenCol":
        """
        Apply `func` to each element returning a new `GenCol`.

        This is the "fmap" of the "Functor" concept.

        :param func: The function to apply.
        :param args: Extra arguments to pass to `func`. Must be other
            `GenCol`s with matching structure.

        :return: A new `GenCol` with the same structure containing the
            results of the operations.
        """

        for arg in args:
            if not arg.is_structural_match(self):
                raise ValueError()

        other_sequence = [s.sequence for s in args]

        return GenCol(
            sequence=[
                func(*params)
                for params in zip(
                    self.sequence,
                    *other_sequence,
                    strict=True,
                )
            ],
            named={
                key: func(value, *(a.named[key] for a in args))
                for key, value in self.named.items()
            },
        )

    def zip(self, *args: "GenCol") -> Generator:
        """
        Zip together corresponding elements of the collections.

        :param args: Extra arguments to pass to `func`. Must be other
            `GenCol`s with matching structure.
        """

        for arg in args:
            if not arg.is_structural_match(self):
                raise ValueError()

        yield from zip(
            self.sequence,
            *(s.sequence for s in args),
            strict=True,
        )

        for key, value in self.named.items():
            yield (value, *(a.named[key] for a in args))

    def foreach(self, func: Callable, *args: "GenCol") -> None:
        """
        Apply `func` to each element.

        :param func: The function to apply.
        :param args: Extra arguments to pass to `func`. Must be other
            `GenCol`s with matching structure.
        """

        for params in self.zip(*args):
            func(*params)

    @classmethod
    def merge(
        cls: type[TGenCol],
        *collections: "GenCol[T]",
    ) -> TGenCol:
        """
        Merge the collections.

        :param collections: Collections to merge. Named keys must be
            unique.

        :raises DuplicateKeysError: If named keys are repeated.
        :return: A new `GenCol` with the merged contents.
        """

        seq = chain.from_iterable(c.sequence for c in collections)
        len_n = sum(len(c.named) for c in collections)
        named_pairs = chain.from_iterable(
            c.named.items() for c in collections
        )
        named = dict(named_pairs)

        if len(named) != len_n:
            raise DuplicateKeysError()

        return cls(sequence=list(seq), named=named)
