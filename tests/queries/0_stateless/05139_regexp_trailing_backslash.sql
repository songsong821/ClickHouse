-- The SQL literal 'abc\\' is the four-character regexp `abc\`, which re2 rejects (`trailing \`).
-- It must be rejected instead of being answered as a search for the literal `abc`.

SELECT match('abcd', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('abcd', '\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT extractAll('abcd', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT countMatches('abcabc', 'abc\\'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT splitByRegexp('abc\\', 'xabcy'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT extractAllGroups('abcd', '(abc)\\'); -- { serverError CANNOT_COMPILE_REGEXP }

-- An escaped backslash is an ordinary literal, and a pattern that ends in one is valid.
SELECT match('abc\\', 'abc\\\\');
SELECT extractAll('a\\b\\c', 'a\\\\');
SELECT countMatches('a\\b\\c', '\\\\');
SELECT match('abcd', 'abc');
