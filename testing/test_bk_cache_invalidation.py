"""
Business Knowledge is merged into live chat retrieval (chat.py Phase 2).
RAG answers are cached under a key that includes kb_version. FAQ uploads
already bump that version when embeddings finish; BK ingestion used to skip
the bump, so a customer could keep getting a cached "I don't know" / old
FAQ answer for up to 24h after importing a shipping policy.

These checks:
  1. Run the real cache_utils bump/key logic (no Redis) to prove a bump
     actually moves the cache key.
  2. Confirm the BK embedding job and URL-replace path call bump_kb_version
     in the right order, by walking the real source AST — so a later
     refactor that drops the call fails this file the same way a missed
     import failed test_register_webhooks.py.

Run with: python testing/test_bk_cache_invalidation.py
"""
import ast
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

# Force the in-process fallback so this doesn't depend on Redis.
os.environ.pop('REDIS_URL', None)

import cache_utils

passed = 0
failed = 0


def check(label, condition):
    global passed, failed
    if condition:
        passed += 1
        print(f'  PASS  {label}')
    else:
        failed += 1
        print(f'  FAIL  {label}')


def _calls_in_function(tree, func_name):
    """Return attribute/name of each Call in source order inside func_name."""
    names = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            for child in ast.walk(node):
                if isinstance(child, ast.Call):
                    fn = child.func
                    if isinstance(fn, ast.Attribute):
                        names.append(fn.attr)
                    elif isinstance(fn, ast.Name):
                        names.append(fn.id)
            break
    return names


print('cache_utils — bump must change the RAG cache key')
client_id = 'bk-cache-invalidation-test'
question = "What is your shipping policy?"
v1 = cache_utils.get_kb_version(client_id)
k1 = cache_utils.make_cache_key(client_id, v1, question)
v2 = cache_utils.bump_kb_version(client_id)
k2 = cache_utils.make_cache_key(client_id, v2, question)
check('bump increments kb_version', v2 == v1 + 1)
check('same question maps to a different key after bump', k1 != k2)
check('new key includes the new version', f':kb:{v2}:' in k2)


print()
print('blueprints/business_knowledge.py — BK mutations bump kb_version')
src_path = os.path.join(ROOT, 'blueprints', 'business_knowledge.py')
with open(src_path, encoding='utf-8') as f:
    tree = ast.parse(f.read(), filename=src_path)

job_calls = _calls_in_function(tree, '_run_knowledge_embedding_job')
check(
    'embedding job calls bump_kb_version',
    'bump_kb_version' in job_calls,
)
if 'finalize_knowledge_import_job' in job_calls and 'bump_kb_version' in job_calls:
    check(
        'bump happens after embeddings are finalized (not before)',
        job_calls.index('finalize_knowledge_import_job')
        < job_calls.index('bump_kb_version'),
    )
else:
    check('bump happens after embeddings are finalized (not before)', False)

url_calls = _calls_in_function(tree, 'import_business_knowledge_url')
check(
    'URL re-import calls bump_kb_version after deleting old chunks',
    'bump_kb_version' in url_calls and 'delete_source_chunks' in url_calls
    and url_calls.index('delete_source_chunks') < url_calls.index('bump_kb_version'),
)

print()
print(f'{passed} passed, {failed} failed')
sys.exit(1 if failed else 0)
