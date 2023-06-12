#pragma once

#ifndef NDEBUG
#include <cassert>

#define DASSERT_EQ(x, y) assert((x) == (y))
#define DASSERT_GE(x, y) assert((x) >= (y))
#define DASSERT_GT(x, y) assert((x) > (y))
#define DASSERT_LE(x, y) assert((x) <= (y))
#define DASSERT_LT(x, y) assert((x) < (y))
#define DASSERT_TRUE(x) assert(x)
#define DASSERT_FALSE(x) assert(!(x))
#else
#define DASSERT_EQ(x, y)
#define DASSERT_GE(x, y)
#define DASSERT_GT(x, y)
#define DASSERT_LE(x, y)
#define DASSERT_LT(x, y)
#define DASSERT_TRUE(x)
#define DASSERT_FALSE(x)
#endif

#define ASSERT_EQ(x, y) assert((x) == (y))
#define ASSERT_GE(x, y) assert((x) >= (y))
#define ASSERT_GT(x, y) assert((x) > (y))
#define ASSERT_LE(x, y) assert((x) <= (y))
#define ASSERT_LT(x, y) assert((x) < (y))
#define ASSERT_TRUE(x) assert(x)
#define ASSERT_FALSE(x) assert(!(x))

#ifdef _MSVC_LANG
#define LIKELY(x) !!(x)
#define UNLIKELY(x) !!(x)
#else
#define LIKELY(x) __builtin_expect(!!(x), 1)
#define UNLIKELY(x) __builtin_expect(!!(x), 0)
#endif