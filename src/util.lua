function aux_union_list(list, aux_list, ret_list)
    for _, v in pairs(list) do
        if not aux_list[v] then
            aux_list[v] = true
            table.insert(ret_list, v)
        end
    end
end

function union_list(list_x, list_y)
    local aux_list = {}
    local ret_list = {}

    aux_union_list(list_x, aux_list, ret_list)
    aux_union_list(list_y, aux_list, ret_list)

    return ret_list
end

return {
    union_list = union_list,
}
