module ScanValidate
using Printf;

# TODO: Return (num_valid_args, error_message)
function validate_args(scan::Function, expected_args::Core.SimpleVector)::String
  fn_args = Base.arg_decl_parts(first(methods(scan)))[2]
  # We already know the function name is "scan"
  popfirst!(fn_args)
  if length(expected_args) != length(fn_args)
    return @sprintf("Expected `scan` to take %d args but it's defined to take %d", length(expected_args), length(fn_args))
  end
  for (expected_arg, (arg_name, actual_arg)) in zip(expected_args, fn_args)
    if expected_arg != actual_arg
      return @sprintf("Expected arg %s to be of type %s but got \"%s\" in function `scan`", arg_name, expected_arg, actual_arg)
    end
  end
  return ""
end
# scan(a::UInt8, b::UInt8)=a
# println(validate_args(scan, Core.svec("UInt8", "UInt8")))
end

