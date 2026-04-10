project('pg_volvec', 'c', 'cpp',
  version: '1.0',
  default_options: ['cpp_std=c++17', 'c_std=c11']
)

pg_config = find_program('pg_config', required: true)

# Query PostgreSQL installation paths
includedir_server = run_command(pg_config, '--includedir-server', check: true).stdout().strip()
pkglibdir = run_command(pg_config, '--pkglibdir', check: true).stdout().strip()
sharedir = run_command(pg_config, '--sharedir', check: true).stdout().strip()
extensiondir = join_paths(sharedir, 'extension')

# Add PostgreSQL server headers to include path
add_project_arguments('-I' + includedir_server, language: ['c', 'cpp'])

inc = include_directories('src/bridge', 'src/engine')

pg_volvec_sources = files(
  'src/bridge/pg_volvec.c',
  'src/bridge/state.c',
  'src/bridge/execute.cpp',
  'src/engine/expr.cpp',
  'src/engine/executor.cpp',
)

optimize_args = [
  '-O3',
  '-march=native',
  '-ftree-vectorize',
  '-funroll-loops',
  '-ffast-math',
]

add_project_arguments(optimize_args, language: ['c', 'cpp'])

mod_link_args = []
if host_machine.system() == 'darwin'
  mod_link_args += ['-undefined', 'dynamic_lookup']
endif

deps = []

# LLVM integration
llvm_dep = dependency('llvm', required: false, method: 'config-tool')
if llvm_dep.found()
  pg_volvec_sources += files('src/engine/llvmjit_deform_datachunk.cpp', 'src/engine/llvmjit_expr.cpp')
  deps += llvm_dep
endif

# Build the extension shared module
pg_volvec = shared_module('pg_volvec',
  pg_volvec_sources,
  include_directories: inc,
  dependencies: deps,
  name_prefix: '',
  link_args: mod_link_args,
  install: true,
  install_dir: pkglibdir
)

# Install extension control and SQL script
install_data(
  'pg_volvec.control',
  'pg_volvec--1.0.sql',
  install_dir: extensiondir
)
