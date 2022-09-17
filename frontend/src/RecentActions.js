import React, { useState, useEffect } from 'react';

import './codeforcesCSS/typography.css'
import './codeforcesCSS/cf.css'

import axios from 'axios';
const RecentActions = () => {
  const [products, setProducts] = useState([]);
  useEffect(() => {
    fetchProducts();
  }, []);
  const fetchProducts = () => {
    let v1Public = '/api/v1/public'
    let endpoint = '/user/activity/recent-actions'
    let uuid = 'f84d38d4-a949-40fd-a3b2-12f3cdf563e2'
    let timestamp = '0'
    let query = '?uuid=' + uuid + '&startTimestamp=' + timestamp
    let url = v1Public + endpoint +  query
    axios
      .get(url)
      .then((res) => {
        console.log(res);
        setProducts(res.data);
      })
      .catch((err) => {
        console.log(err);
      });
  };

  const extractComment = (activity) => {
    if (activity.hasOwnProperty('comment')) {
      return activity.comment.text
    }
    return ""
  }

  const getIdentifier = (activity) => {
    if (activity.hasOwnProperty('blogEntry') && activity.hasOwnProperty('comment')) {
      return activity.blogEntry.id + '_' + activity.comment.id
    }

    // TODO: Figure out unique identifier in case of missing entries.
    return ""
  }

  const extractCommentLink = (activity) => {
    return 'https://codeforces.com/blog/entry/' +
      activity.blogEntry.id + '#comment-' + activity.comment.id
  }

  const htmlToText = (html) => {
    var temp = document.createElement('div');
    temp.innerHTML = html;
    return temp.textContent; // Or return temp.innerText if you need to return only visible text. It's slower.
  }

  return (
    <div>
      <h1>Recent Actions</h1>
      <div className='item-container'>
        {products.map((activity) => (
          // TODO: Figure out an appropriate key.
          <div key={getIdentifier(activity)}>
            {/* TODO: Sanitize the inner HTML*/}
            <div className="cf-table-wrapper">
              <table className="comment-table">
                <tbody>
                <tr>
                  <td style={{width: 8 + 'em'}}>
                    <div>
                      <a href={"https://codeforces.com/profile/" + activity.comment.commentatorHandle} style={{position: 'relative'}}>
                        <img src="https://cdn-userpic.codeforces.com/1856032/avatar/73ea75ced650eedc.jpg" alt="" /> </a>
                      <div><a href={"https://codeforces.com/profile/" + activity.comment.commentatorHandle}>{activity.comment.commentatorHandle}</a></div>
                    </div>
                  </td>

                  <td className="cf-td">
                    <div>
                      On <a href={"https://codeforces.com/profile/" + activity.blogEntry.authorHandle}>  {activity.blogEntry.authorHandle}</a> → <a href={extractCommentLink(activity)}> {htmlToText(activity.blogEntry.title)}</a>
                    </div>

                    <div dangerouslySetInnerHTML={{__html: extractComment(activity)}} />
                  </td>
                </tr>
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default RecentActions;