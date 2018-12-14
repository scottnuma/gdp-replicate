from gdb_log_utils import *
import os

os.system('mkdir -p /tmp/gdp/')
os.system('rm -f /tmp/gdp/*')


"""
0 - a - b - c - d - e
"""
graph = [
         (get_blob('a'), 0, 0, 0, get_blob('0'), get_blob('value'), get_blob('sig')),
         (get_blob('b'), 1, 0, 0, get_blob('a'), get_blob('value'), get_blob('sig')),
         (get_blob('c'), 2, 0, 0, get_blob('b'), get_blob('value'), get_blob('sig')),
         (get_blob('d'), 3, 0, 0, get_blob('c'), get_blob('value'), get_blob('sig')),
         (get_blob('e'), 4, 0, 0, get_blob('d'), get_blob('value'), get_blob('sig'))]

write_graph_to_db(graph, '/tmp/gdp/simple_long.glob')

"""
0 - a - b 
"""
graph = [
         (get_blob('a'), 0, 0, 0, get_blob('0'), get_blob('value'), get_blob('sig')),
         (get_blob('b'), 0, 0, 0, get_blob('a'), get_blob('value'), get_blob('sig'))]

write_graph_to_db(graph, '/tmp/gdp/simple_short.glob')
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


write_graph_to_db(graph, '/tmp/gdp/hole_and_branch.glob')

"""
0 - a - b - c - d
"""
graph = [
          (get_blob('a'), 0, 0, 0, get_blob('0'), get_blob('value'), get_blob('sig')),
          (get_blob('b'), 0, 0, 0, get_blob('a'), get_blob('value'), get_blob('sig')),
          (get_blob('c'), 0, 0, 0, get_blob('b'), get_blob('value'), get_blob('sig')),
          (get_blob('d'), 0, 0, 0, get_blob('c'), get_blob('value'), get_blob('sig'))]

write_graph_to_db(graph, '/tmp/gdp/main.glob')

