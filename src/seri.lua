function pack(root_table)
    local mark = {}
    local assign = {}

    local function seri_key(key)
        if type(key) == "number" then
            return "[" .. key .. "]"
        else
            return "[" .. string.format("%q", key) .. "]"
        end
    end

    local function seri_table(t, parent)
        mark[t] = parent
        local tmp = {}

        for k, v in pairs(t) do
            local key = seri_key(k)

            if type(v) == "table" then
                local dot_key = parent .. key
	     		if mark[v] then
                    table.insert(assign, dot_key .. "=" .. mark[v])
	     		else
                    table.insert(tmp, key .. "=" .. seri_table(v, dot_key))
	     		end
	    	elseif type(v) == "string" then
                table.insert(tmp, key .. "=" .. string.format('%q', v))
	    	elseif type(v) == "number" or type(v) == "boolean" then
                table.insert(tmp, key .. "=" .. tostring(v))
	    	end
   		end
        return "{" .. table.concat(tmp, ",") .. "}"
	end
    return "do local ret=" .. seri_table(root_table, "ret") .. table.concat(assign, " ") .. " return ret end"
end

function unpack(seri_str)
	local f = loadstring(seri_str)
	if f then
	   return f()
	end
end

return {
    pack = pack,
    unpack = unpack
}
