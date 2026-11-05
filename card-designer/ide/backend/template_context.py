"""Project only the fields requested by current gallery templates."""
from jinja2 import Environment, TemplateError, nodes
from .render_context import build_context

ASSET_SOURCES = {
    'photo': ('lambdaWalker/ds.photo_id', 'image'),
    **{field: ('lambdaWalker/ds.machine_readable_code', field)
       for field in ('slim_barcode', 'barcode', 'qrcode', 'pdf417')},
}


def requirements(template, css=''):
    try:
        trees = [Environment().parse(source) for source in (template, css)]
    except TemplateError:
        # Let the normal renderer report the precise template/CSS line error.
        return None
    names = [node.name for tree in trees for node in tree.find_all(nodes.Name)]
    if 'assets' not in names:
        return None
    data_accesses = [node for tree in trees for node in tree.find_all(nodes.Getattr)
                     if isinstance(node.node, nodes.Name) and node.node.name == 'data']
    compatibility = 'ds' in names or 'person' in names or names.count('data') != len(data_accesses) or any(node.attr not in ('id', 'index') for node in data_accesses)
    assets = {node.attr for tree in trees for node in tree.find_all(nodes.Getattr)
              if isinstance(node.node, nodes.Name) and node.node.name == 'assets'}
    for tree in trees:
        for node in tree.find_all(nodes.Getitem):
            if isinstance(node.node, nodes.Name) and node.node.name == 'assets':
                if not isinstance(node.arg, nodes.Const) or not isinstance(node.arg.value, str):
                    raise ValueError('Template asset keys must be literal names')
                assets.add(node.arg.value)
    unknown = assets - ASSET_SOURCES.keys()
    if unknown:
        raise ValueError('Unknown template assets: ' + ', '.join(sorted(unknown)))
    address = any(isinstance(node.node, nodes.Name) and node.node.name == 'gen' and node.attr == 'address' for tree in trees for node in tree.find_all(nodes.Getattr))
    return {'assets': sorted(assets), 'identity': 'identity' in names, 'card': 'card' in names, 'address': address, 'generators': 'gen' in names, 'compatibility': compatibility}


def project_context(snapshot, design, person, rng):
    requested = requirements(design.template, design.css)
    if requested is None:
        return build_context(snapshot, design, person, rng)
    if requested['compatibility']:
        context, summary = build_context(snapshot, design, person, rng)
        assets = {key: context['ds'].image(ASSET_SOURCES[key][0], context['data']['id'], ASSET_SOURCES[key][1]) for key in requested['assets']}
        context['assets'] = assets
        summary['assets'] = assets
        return context, summary
    context, summary = build_context(snapshot, design, person, rng,
                                     publish=set(), include_identity=requested['identity'] or requested['card'])
    ds = context.pop('ds')
    assets = {key: ds.image(*[ASSET_SOURCES[key][0], context['data']['id'], ASSET_SOURCES[key][1]])
              for key in requested['assets']}
    bindings = {dataset: {'count': group['count'], 'records': [
        {'record_id': record['id'], 'index': record['index']} for record in group['records'].values()]}
                for dataset, group in summary['datasets'].items()}
    context['assets'] = assets
    context.pop('person', None)
    if not requested['generators']:
        context.pop('gen', None)
    context['data'] = {key: context['data'][key] for key in ('id', 'index')}
    if not requested['identity']:
        context.pop('identity', None)
    if not requested['card']:
        context.pop('card', None)
    compact = {key: summary[key] for key in ('common', 'env', 'missing', 'selection')}
    if requested['generators']:
        compact['generator'] = summary['generator']
    compact.update(data=context['data'], assets=assets, bindings=bindings, projection='template')
    if requested['identity']:
        compact['identity'] = context['identity']
    if requested['identity'] or requested['card']:
        compact['identity_source'] = summary['identity_source']
    if requested['card']:
        compact['card'] = context['card']
    return context, compact
