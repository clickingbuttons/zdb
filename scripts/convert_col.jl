function get_partition_dirs(table_name)
  meta_path = joinpath("data", table_name, "_meta")
  meta_file = open(meta_path, "r")
  meta_data = read(meta_file, String)
  # Could parse JSON but this is faster + easier
  dir_regex = r"\"dir\":\s*\"(.*)\""
  sort([m.captures[1] for m in eachmatch(dir_regex, meta_data)])
end

function get_column_type(col_ext)
  if col_ext == ".u64"
    UInt64
  else
    println("Unknown column type ", col_ext)
    exit(2)
  end
end

function get_type_column(col_type)
  if col_type == UInt32
    ".u32"
  else
    println("Unknown column type ", col_type)
    exit(3)
  end
end

function convert_column(partition_dirs, col_name, to_type)
  col_files = readdir(partition_dirs[1])
  col_basenames = filter(d -> startswith(d, col_name * "."), col_files)
  if length(col_basenames) > 1
    println("Found multiple columns ", col_basenames)
    exit(1)
  end
  (col_basename, col_ext) = splitext(col_basenames[1])
  for p in partition_dirs
    col_path = joinpath(p, col_basename * col_ext)
    println(col_path)
    io_in = open(col_path, "r")
    data = UInt8[]
    readbytes!(io_in, data, Inf)
    close(io_in)
    data = reinterpret(get_column_type(col_ext), data)
    data = convert(Array{to_type}, data)
    println(length(data))
    mv(col_path, col_path * ".bak")
    new_path = joinpath(p, col_basename * get_type_column(to_type))
    println(new_path)
    io_out = open(new_path, "w")
    write(io_out, data)
    close(io_out)
  end
end

function convert_table_column(table_name, col_name, to_type)
  dirs = get_partition_dirs(table_name)
  convert_column(dirs, col_name, to_type)
end

convert_table_column("agg1m", "volume", UInt32)

