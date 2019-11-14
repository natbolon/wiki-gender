import re


def clear_overview(dirty_overview):
    # remove the <ref> </ref>
    overview = re.sub('<ref.*</ref>', '', dirty_overview)
    overview = re.sub('<ref.*/>', '', overview)

    # remove {{ }} and what is inside
    overview = re.sub('[\{].*[\}]', '', overview)

    # remove [[ ]] and keep what is inside and for the cases like [[abc | def]] keep only def and remove the rest
    overview = re.sub(r'\[\[(?:[^\]|]*\|)?([^\]|]*)\]\]', r'\1', overview)

    # remove ''' ''' 
    overview = re.sub('\'{2,3}', '', overview)

    # remove \n
    overview = re.sub('\n', '', overview)
    
    return overview