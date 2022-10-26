MUNICIPALITY = ['北京市', '天津市', '上海市', '重庆市', '香港', '澳门']

init_province = None
init_city = None


def get_province(addr_str, location_tree):
    if len(addr_str) < 2:
        return {
            'province': '',
            'city': '',
            'district': '',
            'detail': '',
        }

    start, end = 0, 2
    sub_province = addr_str[start: end]
    province = {}
    while len(sub_province) == 2:
        children = location_tree.get('children')
        for province_item in children:
            province_item_name = province_item.get('name')
            if province_item_name.find(sub_province) == 0:
                province = province_item
                break
        if province:
            break
        start+=1
        end+=1
        sub_province = addr_str[start: end]

    if province:
        province_name = province.get('name')

        while addr_str[start: end] in province_name and end <= len(addr_str):
            end += 1
        return {
            'province': province,
            'string': addr_str[end-1:],
        }
    return {
        'province': province,
        'string': addr_str[0:]
    }

def get_city(province, addr_str, location_tree):
    city, start, end = {}, 0, 2
    if len(addr_str) == 0:
        return {
            'province': province,
            'city': '',
            'district': '',
            'detail': ''
        }
    sub_city = addr_str[start: end]
    if province:
        if province.get('name') in MUNICIPALITY:
            city = province.get('children')[0]
            name = city.get('name')
            if sub_city not in name:
                return {
                    'province': province,
                    'city': city,
                    'string': addr_str[start:]
                }
            while addr_str[start:end] in name and end <= len(addr_str):
                end += 1
            return {
                'province': province,
                'city': city,
                'string': addr_str[end-1:]
            }

        while len(sub_city) == 2:
            children = province.get('children')
            for item in children:
                item_name = item.get('name')
                if item_name.find(sub_city) == 0:
                    city = item
                    break

            if city:
                break
            start += 1
            end += 1
            sub_city = addr_str[start: end]

        if city:
            city_name = city.get('name')
            while addr_str[start:end] in city_name and end <= len(addr_str):
                end += 1
            return {
                'province': province,
                'city': city,
                'string': addr_str[end-1:]
            }
        return {
            'province': province,
            'city': city,
            'string': addr_str,
        }
    else:
        city, new_province = {}, {}
        while len(sub_city) == 2:
            location_children = location_tree.get('children')
            for province_item in location_children:
                province_children = province_item.get('children')
                for city_item in province_children:
                    city_name = city_item.get('name')
                    if sub_city.find(city_name) == 0:
                        city = city_item
                        new_province = province_item
                        break

            if city:
                break

            start += 1
            end += 1
            sub_city = addr_str[start: end]

        if city:
            city_name = city.get('name')
            while addr_str[start:end] in city_name and end <= len(addr_str):
                end += 1
            return {
                'province': new_province,
                'city': city,
                'string': addr_str[end-1:]

            }
        else:
            return {
                'province': province,
                'city': city,
                'string': addr_str

            }

def get_district(province, city, addr_str, location_tree):
    if not addr_str:
        return {
            'province': province,
            'city': city,
            'district': '',
            'detail': ''
        }

    start, end, district = 0, 2, {}
    sub_district = addr_str[start:end]

    if province:
        if city:
            while len(sub_district) == 2:
                city_children = city.get('children')
                for district_item  in city_children:
                    district_name = district_item.get('name')
                    if district_name.find(sub_district) == 0:
                        district = district_item
                        break

                if district:
                    break

                start+=1
                end+=1
                sub_district = addr_str[start:end]

            if district:
                district_name = district.get('name')
                while addr_str[start:end] in district_name and end <= len(addr_str):
                    end += 1
                return {
                    'province': province,
                    'city': city,
                    'district': district,
                    'string': addr_str[end-1:]
                }
            else:
                return {
                    'province': init_province,
                    'city': init_city,
                    'district': district,
                    'string': addr_str
                }
        else:
            result = {
                'province': province,
                'city': city,
                'district': ''
            }

            province_children = province.get('children')

            for index in range(len(province_children)):
                city = province_children[index]
                result = get_district(province, city, addr_str, location_tree)
                if result.get('district'):
                    break

            return result

    else:
        result = {
            'provice': province,
            'city': city,
        }
        provinces = location_tree.get('children')
        for province_index in range(len(provinces)):
            target_province = provinces[province_index]
            cities = target_province.get('children')
            for city_index in range(len(cities)):
                result = get_district(target_province, cities[city_index], addr_str, location_tree)
                if result.get('district'):
                    break

            if result.get('district'):
                break

        return result

def parse_geolocation_from_tree(location_tree, addr_str):

    global  init_province, init_city
    if len(addr_str) < 2:
        return {
            'province': None,
            'city': None,
            'district': None,
            'detail': addr_str
        }

    string = addr_str
    province_result = get_province(string, location_tree)
    province = province_result.get('province')
    string = province_result.get('string')

    city_result = get_city(province, string, location_tree)
    province = city_result.get('province')
    city = city_result.get('city')
    string = city_result.get('string')

    init_province = province
    init_city = city

    district_result = get_district(province, city, string, location_tree)
    district = district_result.get('district')
    province = district_result.get('province')
    city = district_result.get('city')
    string = district_result.get('string')


    return {
        'province': province and province.get('name') or None,
        'city': city and city.get('name') or None,
        'district': district and district.get('name') or None,
        'detail': string or ''
    }

