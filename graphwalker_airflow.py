import hashlib
import json
import sys

import airflow.models as af_models

def _make_hash_id(s):
    return hashlib.sha1(s.encode('utf-8')).hexdigest()


def _make_task_id(task):
    return _make_hash_id(
        '{type}({dag_id}.{task_id})'.format(
            type=task.task_type,
            dag_id=task.dag_id,
            task_id=task.task_id
        )
    )


def _make_edge_id(source_vertex, dest_vertex, relation_type):
    return hashlib.sha1(
        '{}({},{})'.format(relation_type, source_vertex['id'], dest_vertex['id']).encode('utf-8')
    ).hexdigest()


def export_dag_graph(dag_bag):
    vertices = {}
    edges = {}

    for dag_id, dag in dag_bag.dags.items():
        # add a single vertex representing the DAG
        dag_label = 'DAG({})'.format(dag_id)
        dag_graphwalker_id = _make_hash_id(dag_label)
        dag_vertex = {
            'id': dag_graphwalker_id,
            'label': dag_label,
            'searchableComponents': [dag.dag_id, 'dag'],
            'properties': {
                'dag_id': dag.dag_id,
                'description': dag.description,
                'vertex_types': [
                    'airflow',
                    'dag'
                ],
                'module': dag.full_filepath
            }
        }
        vertices[dag_graphwalker_id] = dag_vertex

        # add vertices for all available tasks
        for task in dag.tasks:
            id = _make_task_id(task)
            new_task_vertex = {
                'id': id,
                'label': task.task_id,
                'searchableComponents': [task.task_id, task.task_type, 'task'],
                'properties': {
                    'task_id': task.task_id,
                    'task_type': task.task_type,
                    'vertex_types': [
                        'airflow',
                        'task'
                    ]
                }
            }
            vertices[id] = new_task_vertex

        # create relationships for all starting tasks
        starting_tasks = [t for t in dag.tasks if not t.upstream_list]
        source_vertex = dag_vertex
        label = 'starts'
        for task in starting_tasks:
            dest_vertex = vertices[_make_task_id(task)]
            edge_id = _make_edge_id(source_vertex, dest_vertex, label)
            new_edge = {
                'id': edge_id,
                'label': label,
                'source': source_vertex['id'],
                'dest': dest_vertex['id'],
                'properties': {
                    'type': label,
                    'relation_types': [
                        'airflow',
                        'starting_tasks'
                    ]
                }
            }
            edges[edge_id] = new_edge

        # walk the dependency tree to add more vertices
        label = 'follows'
        for source_task in dag.tasks:
            source_vertex = vertices[_make_task_id(source_task)]
            for dest_task in source_task.downstream_list:
                dest_vertex = vertices[_make_task_id(dest_task)]
                edge_id = _make_edge_id(source_vertex, dest_vertex, label)
                if edge_id not in edges:
                    new_edge = {
                        'id': edge_id,
                        'label': label,
                        'source': source_vertex['id'],
                        'dest': dest_vertex['id'],
                        'properties': {
                            'type': label,
                            'relation_types': [
                                'airflow',
                                'task_dependency'
                            ]
                        }
                    }
                    edges[edge_id] = new_edge

    return {
        'vertices': list(vertices.values()),
        'edges': list(edges.values())
    }


def main(dag_bag_folder, output_filename):
    bag = af_models.DagBag(dag_bag_folder)

    graph = export_dag_graph(bag)

    with open(output_filename, 'w') as f:
        json.dump(graph, f, indent=2)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
