from gdb_log_utils import *
import os

DB_DIR = "example_db"
os.system('mkdir -p ' + DB_DIR)
os.system('rm -f %s/*' % DB_DIR)


"""
0 - a - b - c - d - e
"""
graph = [
         (get_blob('a'), 0, 0, 0, get_blob('0'), get_blob('value'), get_blob('sig')),
         (get_blob('b'), 1, 0, 0, get_blob('a'), get_blob('value'), get_blob('sig')),
         (get_blob('c'), 2, 0, 0, get_blob('b'), get_blob('value'), get_blob('sig')),
         (get_blob('d'), 3, 0, 0, get_blob('c'), get_blob('value'), get_blob('sig')),
         (get_blob('e'), 4, 0, 0, get_blob('d'), get_blob('value'), get_blob('sig'))]

write_graph_to_db(graph, '%s/simple_long.glob' % DB_DIR)

"""
0 - a - b 
"""
graph = [
         (get_blob('a'), 0, 0, 0, get_blob('0'), get_blob('value'), get_blob('sig')),
         (get_blob('b'), 0, 0, 0, get_blob('a'), get_blob('value'), get_blob('sig'))]

write_graph_to_db(graph, '%s/simple_short.glob' % DB_DIR)
"""
           - f
         /
0 - a - b - c - [] - e
"""
graph = [
          (get_blob('a'), 0, 0, 0, get_blob('0'), get_blob('value'), get_blob('sig')),
          (get_blob('b'), 0, 0, 0, get_blob('a'), get_blob('value'), get_blob('sig')),
          (get_blob('f'), 0, 0, 0, get_blob('b'), get_blob('value'), get_blob('sig')),
          (get_blob('c'), 0, 0, 0, get_blob('b'), get_blob('value'), get_blob('sig')),
          (get_blob('e'), 0, 0, 0, get_blob('d'), get_blob('value'), get_blob('sig'))]


write_graph_to_db(graph, '%s/hole_and_branch.glob' % DB_DIR)

"""
0 - a - b - c - d
"""
graph = [
          (get_blob('a'), 0, 0, 0, get_blob('0'), get_blob('value'), get_blob('sig')),
          (get_blob('b'), 0, 0, 0, get_blob('a'), get_blob('value'), get_blob('sig')),
          (get_blob('c'), 0, 0, 0, get_blob('b'), get_blob('value'), get_blob('sig')),
          (get_blob('d'), 0, 0, 0, get_blob('c'), get_blob('value'), get_blob('sig'))]

write_graph_to_db(graph, '%s/main.glob' % DB_DIR)

