''' Basic network extraction script ''' 
from collections import defaultdict
import csv,pickle,json

class UnknownMethodException(Exception):
    pass

class UnsupportedType(Exception):
    pass

def from_csv(filename, *args,**kwargs):
    ''' read a table from csv as a dictionairy ''' 
    return table

def from_pickle(filename):
    return pickle.load(open(filenam))

def from_json(filename):
    return json.load(open(filename))

def find_key(item,key):
    '''
    Helper function to find keys recursivelys

    table: the object (assumed to be a dict) which should contain the key
    key  : the final key, whose corresponding value you whish to have

    for example: there  is a dict
    A = dict( alice=dict(age=23, profession='professor'), bob=dict(age='26',profession='dummy'))
    find_key(A,'profession') will yield 'professor' or 'dummy' !a semi-random pick if the key is in multiple subdicts
    find_key(A,'goals') will yield False 
    
    KEEP IN MIND: the key(s) in the first encountered 'level' are returned!

    '''

    if item.has_key(key): 
        return item[key]
    else:
        combined_subdict = {}
        [combined_subdict.update(d) for d in item if type(d)=='dict']
        if len(combined_subdict)>0:
            return find_key(combined_subdict,key)
        else:
            return False

def extract_edges(table,from_colum, to_colum, split_char='', weight='count', keep_fields_list=[]):
    ''' 
    extract a weighted network 
    
    table     : should be a list of dicts (presumably based on a table with headers)
    from_colum: the colum specifying the 'sender'
    to_colum  : the colum specifying the 'receiver'
    weight    : the method to determine edge-weight, options include
                'count' = weight is the number of edges detected between two nodes
                'newman'= weight is the proportion of edges (directed) example:
                          alice has 5 edges, 1 to bob, thus alice --0.2--> bob.
                          ! this is calculated over the number of to-from matches!
    split_char: optional character to split the colums with, for example:
                a 'to' field on emails is 'alice@email.com ; bob@email.com',
                the split_char=';' would yield 2 instead of 1 edge, one to alice
                and one to bob!
    keep_field: optional list of fieldnames that should be included as link-properties.
                Example: the 'subject' columns of all emails between 'from'=Alice and 'to'=bob
    
    PLEASE NOTE: if the 'from' and 'to' colum are different type, 
    such as people & events, the resulting edgeset will be "two-mode" data.
    I.e.: the 'from' category consists of a different type of nodes as compared
    to the 'to' category. ( one mode= Alice -1->  Bob, two mode = Alice -1-> 4chan ).

    You can project a two-mode network to a one mode network using 'flatten_two_to_one_mode'
    function. 
    '''
    
    # Get the edges from the table 
    edges = defaultdict(lambda:defaultdict(lambda:{'weight':0})) # edges is a dict with key = from field, value = 'to':weight(default 0)
    for row in table:
        if find_key(row,from_colum) and find_key(row,to_colum):
            def select(colum):
                item = find_key(row,from_colum)
                if type(item) in ['str','unicode'] and split_char:
                    return item.split(split_char)
                elif type(item) in ['str','unicode']:
                    return [item,]
                elif type(item)=='list'):
                    return item
                else:
                    raise UnsupportedType('Unsupported value type:  %s; perhaps you specified the wrong key?' %type(item))
            fs      = select(from_colum)
            ts      = select(to_colum)
            for f in fs:
                for t in ts:
                    edges[f][t]['weight']+=1
                    
                    # Handle the addition of extra information from specified fields
                    if keep_fields_list:
                        for kf in keep_fields_list:
                            if not edges[f][t].has_key(kf): 
                                edges[f][t][kf]=[]
                            if row.has_key(kf):
                                edges[f][t].append(row[kf])
                            else:
                                edges[f][t].append('')

    # Transform weight based on specified weight algorithm
    if weight.lower()=='newman_weighted':
        for t in edges.keys():
            t_sum = sum(edges[t].values())
            for v in edges[t].values():
                v = t_sum and float(v)/t_sum or 0.0 #divide only if t_sum is bigger than 0
    
    elif weight.lower() in ['newman','newman_binary']:
        for t in edges.keys():
            t_sum = len(edges[t].keys())
            for v in edges[t].values():
                v = t_sum and float(v)/t_sum or 0.0 #divide only if t_sum is bigger than 0        
    
    elif weight.lower()=='count':
        pass
    
    else: # specified weight algorithm was not found
        raise UnknownMethodException('{weight} is not a known method at this time... :-('.format(**locals()))


    return edges

def flatten_two_to_one_mode(input_edge_set, method='count', keep_matches=True, include_self_links=False):
    ''' 
    transform a two mode network into a one mode network 
    
    Takes an edge set (provided by the extract_edges function) and returns 
    a one-mode projection, based on the 'from' nodes specified in the edge set.

    method:       specifies the method of combining weights, includes count, newman, 
                  and minimum.
    keep_matches: adds the overlapping nodes of the second mode as an edge values
    
    '''
    
    output_edges = defaultdict(lambda:defaultdict(lambda:{'weight':0}))
    
    for f in input_edge_set.keys():
        for t in input_edge_set.keys():
            if not include_self_links and t==f:
                continue
            
            #combine links
            overlap = set(input_edge_set[t].keys()).intersection(set(input_edge_set[f].keys()))
            if keep_matches:
                output_edges[f][t]['Based_on']= ';'.join(list(overlap))

            if method.lower() in ['newman_send','newman_binary']: 
                #overlap based on the number of shared keys, relative to the total number of keys for the 'from' node
                output_edges[f][t]['weight'] = len(overlap) / float(len(input_edge_set[f].keys()))

            elif method.lower() == 'count':
                #overlap weight is simply the number of shared items (undirected)
                output_edges[f][t]['weight'] = len(overlap)

            elif method.lower() == 'minimum':
                #overlap is the minimum weight of either the 'to' or 'from' node in ALL overlapping second-mode nodes
                ies = input_edge_set
                output_edges[f][t]['weight'] = min([ies[f][k] for k in overlap].extend([ies[t][k] for k in overlap]))

            elif method.lower() == 'maximum':
                #overlap is the maximum weight of either the 'to' or 'from' node in ALL overlapping second-mode nodes
                ies = input_edge_set
                output_edges[f][t]['weight'] = max([ies[f][k] for k in overlap].extend([ies[t][k] for k in overlap]))

            elif method.lower() == 'newman_received_min':
                #overlap weight of the 'from' node is weighed for all receiving nodes, taking the minimal value,
                # think of parties alice has been to, her relation to bob is expressed as the minimum amount of 
                # time devoted to him at any of the parties they both attended, under the assumption that her time
                # at a party is split equally between all attendees.
                links = []
                for k in overlap:
                    links.append(float(input_edge_set[f][k]['weight'])/len([o for o in input_edge_set if o.has_key(k)])-1)
                output_edges[f][t]['weight']=min(links)

            elif method.lower() == 'newman_received_max':
                #overlap weight of the 'from' node is weighed for all receiving nodes, taking the minimal value,
                # think of parties alice has been to, her relation to bob is expressed as the maximum  amount of 
                # time devoted to him at any of the parties they both attended, under the assumption that her time
                # at a party is split equally between all attendees.
                links = []
                for k in overlap:
                    links.append(float(input_edge_set[f][k]['weight'])/len([o for o in input_edge_set if o.has_key(k)])-1)
                output_edges[f][t]['weight']=max(links)
            else:
                raise UnknownMethodError('The method you specified is not implemented at this time... :-('.format(**locals()))
                
    return output_edges

def links_as_node_property(input_edge_set, delim=';'):
    '''
    Generates a node property which is a string containing all links.

    For instance: a node property which describes the event alice has been to, 
                  so that these are clear after the network has been collapsed to a one-mode network.

    '''
    nodeset = {}
    for f in input_edge_set.keys():
        nodeset[f]=delim.join([ "%s (%s)" %(k,v['weight']) for k,v in input_edge_set[f].iteritems() if v.has_key('weight')])
    return nodeset

def generate_networkx_edges(edges, graph):
    ''' 
    outputs the edges model of extract_network or flatten_two_mode_to_one_mode for networkx graphs 

    example use:
    
    G = networkx.DiGraph()
    G = generate_networkx_edges(edges, G)

    '''
    for f,ts in edges.iteritems():
        for t in ts:
            graph.add_edge( f, t, ts[t])
    return graph
