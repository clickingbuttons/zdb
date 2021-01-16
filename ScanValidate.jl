module ScanValidate
using Printf;

function validate_args(fn_string::String, expected_args::Core.SimpleVector)::String
  try
    fn_args = Meta.parse(fn_string).args[1].args
    if length(expected_args) != length(fn_args) - 1
      return @sprintf("Queried for %d cols but function `scan` takes %d", args, fn_args)
    end
    for (index, expected_arg) in enumerate(expected_args)
      actual_arg = fn_args[index + 1].args
      if length(actual_arg) == 1
        return @sprintf("Argument %s to function `scan` must have correct type annotation", index)
      end
      actual_arg = string(actual_arg[2])
      if expected_arg != actual_arg
        return @sprintf("Expected column %d to be of type %s but got %s in function `scan`", index - 1, expected_arg, actual_arg)
      end
    end
    return ""
  catch err
    return sprint(showerror, err, backtrace())
  end
end
end

